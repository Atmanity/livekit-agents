# Copyright 2026 Atmanity
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import deque
from datetime import timedelta
from typing import Any, Literal

import aiohttp

from livekit import api, rtc
from livekit.agents import (
    DEFAULT_API_CONNECT_OPTIONS,
    NOT_GIVEN,
    AgentSession,
    APIConnectOptions,
    NotGivenOr,
    get_job_context,
)
from livekit.agents.voice.avatar import AvatarSession as BaseAvatarSession, DataStreamAudioOutput
from livekit.agents.voice.io import AudioOutput
from livekit.agents.voice.room_io import ATTRIBUTE_PUBLISH_ON_BEHALF

from .api import (
    AtmeeAPI,
    AtmeeException,
    AvatarSessionInfo,
    WaitFor,
    _check_wait_for,
    _warn_avatar_version,
)
from .log import logger

# The rendering worker resamples nothing: it consumes 16 kHz mono PCM over the
# data stream, so the agent's audio tail is asked for exactly that.
SAMPLE_RATE = 16000
_AVATAR_AGENT_IDENTITY = "atmee-avatar-agent"
_AVATAR_AGENT_NAME = "atmee-avatar-agent"
# Ceiling of a session when none is given (also the most a session can bill
# if the agent dies without the worker ever reporting; server default).
DEFAULT_MAX_DURATION_SECONDS = 3600
# The avatar's token outlives the session ceiling by this much so a slow start
# never joins with an expired token.
_TOKEN_TTL_SLACK = timedelta(minutes=10)
# The renderer says it is alive once a second on this topic, addressed to the
# agent, and that it is retiring (it keeps rendering until a replacement
# joins under its identity and evicts it).
_STATUS_TOPIC = "atmanity-avatar_status"
# Silence from a renderer that has been heard from that means it is gone: a
# hard-killed renderer stays in the room for tens of seconds otherwise.
_HEARTBEAT_TIMEOUT = 5.0
# A renderer that keeps dying is not replaced forever: at most this many
# replacements within the window, each a render billed on its own.
_MAX_REPLACEMENTS = 3
_REPLACEMENT_WINDOW = 600.0
# No replacement is asked for with less of the session's ceiling left.
_MIN_REPLACEMENT_SECONDS = 60.0


class _AvatarAudioOutput(DataStreamAudioOutput):
    """The agent's audio, sent to the avatar at 16 kHz.

    A reply already in progress when the avatar takes over the agent's voice
    keeps the rate it started with, so it is resampled here. The resampler
    belongs to one segment: a flush or a clear drops it with the few
    milliseconds it still holds, rather than carrying them into the next
    reply.

    Once the avatar is lost (:meth:`avatar_lost`) nothing will play or
    acknowledge audio any more. The segments it was given are finished as
    interrupted, at an estimate of how far they played, and later segments
    are taken as played the moment they are flushed, so the agent never
    waits for a receipt that cannot come.
    """

    _resampler: rtc.AudioResampler | None = None
    _resampler_input_rate = 0
    _resampler_channels = 0
    # a segment is open: captured, not flushed yet
    _capturing = False
    # when the avatar said the open segment started playing
    _segment_started_at: float | None = None
    _lost = False
    # of the open segment after the loss: where the avatar was cut off in it
    # (None for a segment begun after the loss), the audio given to it since,
    # and whether it was cleared
    _lost_position: float | None = None
    _lost_pushed = 0.0
    _lost_cleared = False

    async def capture_frame(self, frame: rtc.AudioFrame) -> None:
        if self._lost:
            await self._capture_lost(frame)
            return
        if not self._capturing:
            self._capturing = True
            self._segment_started_at = None
        for resampled in self._to_sample_rate(frame):
            await super().capture_frame(resampled)

    def _to_sample_rate(self, frame: rtc.AudioFrame) -> list[rtc.AudioFrame]:
        resampler = self._resampler
        if frame.sample_rate == SAMPLE_RATE and resampler is None:
            return [frame]
        frames: list[rtc.AudioFrame] = []
        if resampler is not None and (
            frame.sample_rate != self._resampler_input_rate
            or frame.num_channels != self._resampler_channels
        ):
            # the rate changed mid-segment: what the old one holds goes first
            frames.extend(resampler.flush())
            resampler = self._resampler = None
        if frame.sample_rate == SAMPLE_RATE:
            return [*frames, frame]
        if resampler is None:
            resampler = self._resampler = rtc.AudioResampler(
                frame.sample_rate, SAMPLE_RATE, num_channels=frame.num_channels
            )
            self._resampler_input_rate = frame.sample_rate
            self._resampler_channels = frame.num_channels
        frames.extend(resampler.push(frame))
        return frames

    def flush(self) -> None:
        self._resampler = None
        if self._lost:
            self._flush_lost()
            return
        self._capturing = False
        super().flush()

    def clear_buffer(self) -> None:
        self._resampler = None
        if self._lost:
            if self._capturing:
                self._lost_cleared = True
            return
        super().clear_buffer()

    def on_playback_started(self, *, created_at: float) -> None:
        self._segment_started_at = created_at
        super().on_playback_started(created_at=created_at)

    def _played_estimate(self) -> float:
        """How much of the latest segment the avatar played: none before it
        said it started, then at most what it was given."""
        if self._segment_started_at is None:
            return 0.0
        return max(0.0, min(self._pushed_duration, time.time() - self._segment_started_at))

    def avatar_lost(self) -> None:
        """The avatar is gone: finish what it was given, and play nothing more."""
        if self._lost:
            return
        self._lost = True
        self._resampler = None
        position = self._played_estimate()
        writer, self._stream_writer = self._stream_writer, None
        if writer is not None:
            task = asyncio.create_task(self._close_stream(writer))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        # segments already flushed get no receipt now; the open one is
        # finished once it is flushed (_flush_lost)
        flushed = self._pending_playback_count - (1 if self._capturing else 0)
        if self._capturing:
            self._lost_position, self._lost_pushed, self._lost_cleared = position, 0.0, False
        for _ in range(max(flushed, 0)):
            self.on_playback_finished(playback_position=position, interrupted=True)

    async def _close_stream(self, writer: rtc.ByteStreamWriter) -> None:
        try:
            await writer.aclose()
        except Exception:
            logger.debug("could not close the audio stream of a lost atmee avatar", exc_info=True)

    async def _capture_lost(self, frame: rtc.AudioFrame) -> None:
        if not self._capturing:
            # a segment begun after the loss: nobody plays it, so it counts
            # as played from the start
            self._capturing = True
            self._lost_position, self._lost_pushed, self._lost_cleared = None, 0.0, False
            await AudioOutput.capture_frame(self, frame)
            self.on_playback_started(created_at=time.time())
        else:
            await AudioOutput.capture_frame(self, frame)
        self._lost_pushed += frame.duration

    def _flush_lost(self) -> None:
        AudioOutput.flush(self)
        writer, self._stream_writer = self._stream_writer, None
        if writer is not None:
            # opened by a capture that was under way when the avatar was lost
            task = asyncio.create_task(self._close_stream(writer))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        if not self._capturing:
            return
        self._capturing = False
        if self._lost_position is not None:
            # the segment the avatar was cut off in
            self.on_playback_finished(playback_position=self._lost_position, interrupted=True)
        else:
            self.on_playback_finished(
                playback_position=self._lost_pushed, interrupted=self._lost_cleared
            )

    def _handle_playback_started(self, data: rtc.RpcInvocationData) -> str:
        if self._lost:
            return "ok"  # a late receipt from the avatar that left
        return super()._handle_playback_started(data)

    def _handle_playback_finished(self, data: rtc.RpcInvocationData) -> str:
        if self._lost:
            return "ok"
        return super()._handle_playback_finished(data)


class AvatarSession(
    BaseAvatarSession[Literal["avatar_disconnected", "avatar_replaced", "avatar_lost"]]
):
    """An Atmee avatar session: render an Atmee avatar into your agent's room.

    Any Atmee avatar renders by its id: a **v1 avatar** is a talking head
    generated from a single portrait; a **v2 avatar** moves as a whole. Both
    are created in the Atmee studio or with the Atmee API. The
    first v2 session after a quiet period can take a few minutes to start, and
    :meth:`start` waits for it.

    The avatar joins the room as its own participant (``atmee-avatar-agent``)
    publishing on behalf of your agent; your agent's audio output is replaced
    by a data stream to it, and the avatar publishes synced video and audio.
    Billing runs while the avatar is in the room and stops when your agent
    leaves, the room closes, or :meth:`aclose` runs (which also removes the
    avatar participant); a ceiling of ``max_duration_seconds`` bounds it.

    A lost renderer is replaced: when the avatar participant leaves, its
    heartbeat stops, or it says it is retiring, a new render is started under
    the same identity, and the agent's voice moves to it once it is in the
    room (``avatar_replaced``, with the new :class:`AvatarSessionInfo`). A
    retiring renderer keeps playing until its replacement takes over; a dead
    one's render is ended first. Each replacement is a render billed on its
    own, within what is left of ``max_duration_seconds``, and a renderer that
    keeps dying is replaced at most 3 times in 10 minutes.

    Meanwhile, and for good if no replacement can be had (the start
    failed, or the limits above were reached: ``avatar_lost``, with the
    error, if any), the agent never waits on the
    lost avatar: what it was saying ends as interrupted, and if the avatar
    took over a voice the agent already had (started after
    ``agent_session.start``), the agent speaks with that voice again.
    Started before it, the avatar replaced nothing, and the agent goes on
    without audio: handle ``avatar_lost`` to end the call.

    Also emits ``avatar_disconnected`` (the ``rtc.RemoteParticipant``)
    whenever the avatar participant leaves the room while the session runs.
    """

    def __init__(
        self,
        *,
        avatar_id: str,
        api_url: NotGivenOr[str] = NOT_GIVEN,
        api_key: NotGivenOr[str] = NOT_GIVEN,
        avatar_participant_identity: NotGivenOr[str] = NOT_GIVEN,
        avatar_participant_name: NotGivenOr[str] = NOT_GIVEN,
        max_duration_seconds: int = DEFAULT_MAX_DURATION_SECONDS,
        metadata: dict[str, Any] | None = None,
        wait_for: WaitFor = "initializing",
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
        http_session: aiohttp.ClientSession | None = None,
        avatar_version: NotGivenOr[str] = NOT_GIVEN,
    ) -> None:
        """
        Args:
            avatar_id: the Atmee avatar to render, v1 or v2.
            api_key: your ``sk_atmee_...`` key; defaults to ``ATMEE_API_KEY``.
            api_url: API base; defaults to ``ATMEE_API_URL`` or the Atmee cloud.
            avatar_participant_identity: identity the avatar joins with
                (``atmee-avatar-agent``). Set it when one room hosts several
                avatars.
            max_duration_seconds: hard ceiling of the session (default 3600);
                anything but a positive number raises ``ValueError``.
            metadata: free-form JSON attached to the session on the Atmee side.
            wait_for: ``"initializing"`` (default) returns from :meth:`start`
                once the worker acknowledged the start; ``"avatar_joined"``
                blocks until the avatar is in the room. Anything else raises
                ``ValueError``.
            conn_options: connect timeout and retries of the API calls that
                are safe to repeat (reading the avatar before the start, ending
                the session); see :class:`AtmeeAPI`.
            http_session: an aiohttp session to reuse; the job's shared one by
                default, or a private one outside a job (released by
                :meth:`aclose`).
            avatar_version: deprecated and ignored; the avatar's own engine
                decides how it renders.
        """
        _check_wait_for(wait_for)
        _warn_avatar_version(avatar_version)
        if not avatar_id:
            raise AtmeeException("avatar_id is required")
        if int(max_duration_seconds) <= 0:
            raise ValueError(
                f"max_duration_seconds must be a positive number of seconds, "
                f"got {max_duration_seconds!r}"
            )
        super().__init__()
        self._avatar_id = avatar_id
        self._max_duration_seconds = int(max_duration_seconds)
        self._metadata = metadata
        self._wait_for: WaitFor = wait_for
        self._conn_options = conn_options
        # Resolved lazily by AtmeeAPI: the job's shared session inside a
        # worker, a private one (closed by aclose) outside.
        self._api = AtmeeAPI(
            api_key=api_key,
            api_url=api_url,
            conn_options=conn_options,
            session=http_session,
        )

        self._avatar_participant_identity = avatar_participant_identity or _AVATAR_AGENT_IDENTITY
        self._avatar_participant_name = avatar_participant_name or _AVATAR_AGENT_NAME

        self.session_id: str | None = None
        """The Atmee avatar session id once :meth:`start` returned."""
        self.session_info: AvatarSessionInfo | None = None
        """The full start response once :meth:`start` returned."""

        self._end_task: asyncio.Task[None] | None = None
        self._end_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        self._ended = False
        self._started = False
        self._render_requested = False
        # aclose() was called: a start still waiting on its request then asks no
        # more (_stop_start) and ends what the request opens instead of wiring
        # it up. The request itself is never cancelled: the session it may open
        # could not be ended without the id its answer carries.
        self._closed = False
        self._stop_start = asyncio.Event()
        self._start_request: asyncio.Future[AvatarSessionInfo] | None = None
        self._settle_tasks: set[asyncio.Task[bool]] = set()
        self._room_for_events: rtc.Room | None = None
        self._agent_session_for_events: AgentSession[Any] | None = None
        self._aclose_task: asyncio.Task[None] | None = None
        # The audio output the agent speaks through to the avatar, and what it
        # spoke through before (the whole chain and its last sink), so the
        # agent gets its own voice back if the avatar is lost.
        self._audio_output: _AvatarAudioOutput | None = None
        self._audio_agent_session: AgentSession[Any] | None = None
        self._previous_audio: AudioOutput | None = None
        self._previous_audio_tail: AudioOutput | None = None
        # What a replacement render is asked for with, kept from the start.
        self._livekit_url = ""
        self._livekit_api_key = ""
        self._livekit_api_secret = ""
        self._room_name = ""
        self._agent_identity = ""
        self._render_started_at = 0.0
        self._replace_task: asyncio.Task[None] | None = None
        self._replacing_retiring = False
        self._replacements: deque[float] = deque()
        # The renderer in the room now, and the ones replaced: a predecessor
        # its replacement evicted may be reported as leaving after it joined.
        self._renderer_sid: str | None = None
        self._outgoing_sid: str | None = None
        self._replaced_sids: set[str] = set()
        # When the renderer was last heard from; None until it first is.
        self._last_beat: float | None = None
        self._beat_heard = asyncio.Event()
        self._watch_task: asyncio.Task[None] | None = None

    @property
    def avatar_identity(self) -> str:
        return self._avatar_participant_identity

    @property
    def provider(self) -> str:
        return "atmee"

    async def start(
        self,
        agent_session: AgentSession[Any],
        room: rtc.Room,
        *,
        livekit_url: NotGivenOr[str] = NOT_GIVEN,
        livekit_api_key: NotGivenOr[str] = NOT_GIVEN,
        livekit_api_secret: NotGivenOr[str] = NOT_GIVEN,
    ) -> None:
        """Start rendering the avatar into ``room`` for ``agent_session``.

        Mints a LiveKit access token for the avatar participant with YOUR
        project's credentials (``LIVEKIT_URL`` / ``LIVEKIT_API_KEY`` /
        ``LIVEKIT_API_SECRET`` or the arguments), asks Atmee to render into
        the room, and routes the agent's audio to the avatar. Called before
        ``agent_session.start``, the avatar speaks from the agent's first
        word; called after it, the agent speaks with its own voice until the
        avatar takes over. A v1 avatar's start is acknowledged within seconds;
        the first v2 session after a quiet period can take a few minutes, and
        this waits for it. Inside a job it may run before the room is
        connected (with ``wait_for="initializing"``: the avatar joins only
        once your agent is in the room), so a slow start overlaps connecting.

        One-shot: each call creates a separately billed render, so a second
        ``start()`` on the same instance raises. After a failed start, create
        a new ``AvatarSession``. If :meth:`aclose` runs while the start is
        still waiting (the job shuts down, the agent session closes), the
        start asks no more, the render it opened is ended and this raises an
        ``AtmeeException`` with ``code="closed"``. If this call is cancelled,
        its request still finishes in the background and the render it opens
        is ended.
        """
        if self._started:
            raise AtmeeException(
                "AvatarSession.start() was already called; create a new AvatarSession "
                "for another render"
            )
        self._started = True
        await super().start(agent_session, room)
        try:
            await self._start_render(
                agent_session,
                room,
                livekit_url=livekit_url,
                livekit_api_key=livekit_api_key,
                livekit_api_secret=livekit_api_secret,
            )
        except BaseException:
            # Undo what super().start() set up (listeners, join task). Once the
            # render was requested its outcome is uncertain, so close fully:
            # end a session that may exist and remove the avatar participant.
            # Before that, no avatar of ours can be in the room, and removing
            # the participant could disconnect another session's avatar that
            # uses the same identity. Either way the instance stays spent.
            try:
                if self._render_requested:
                    await self.aclose()
                else:
                    await self._release_without_removing_participant()
            except Exception:
                # never let a teardown error replace the real start failure
                logger.warning("cleanup after a failed atmee avatar start failed", exc_info=True)
            raise

    async def _release_without_removing_participant(self) -> None:
        async with self._close_lock:
            room = self._room
            if room is not None:
                room.off("connection_state_changed", self._on_connection_state_changed)
                # the base aclose() removes the avatar identity from a
                # connected room; with no room it only drops its listener
                # and cancels the join task
                self._room = None
            await super().aclose()
            await self._api.aclose()

    async def _start_render(
        self,
        agent_session: AgentSession[Any],
        room: rtc.Room,
        *,
        livekit_url: NotGivenOr[str],
        livekit_api_key: NotGivenOr[str],
        livekit_api_secret: NotGivenOr[str],
    ) -> None:

        livekit_url = livekit_url or (os.getenv("LIVEKIT_URL") or NOT_GIVEN)
        livekit_api_key = livekit_api_key or (os.getenv("LIVEKIT_API_KEY") or NOT_GIVEN)
        livekit_api_secret = livekit_api_secret or (os.getenv("LIVEKIT_API_SECRET") or NOT_GIVEN)
        if not livekit_url or not livekit_api_key or not livekit_api_secret:
            raise AtmeeException(
                "livekit_url, livekit_api_key, and livekit_api_secret must be set "
                "by arguments or environment variables"
            )

        job_ctx = get_job_context(required=False)
        local_participant_identity = (
            job_ctx.local_participant_identity
            if job_ctx is not None
            else room.local_participant.identity
        )
        # A room learns its name when it connects; the job knows it before, so
        # the render (minutes for a v2 avatar's first start) can be asked for
        # while the agent is still connecting.
        room_name = room.name or (job_ctx.job.room.name if job_ctx is not None else "")
        if not room_name:
            raise AtmeeException(
                "the room has no name yet: connect it before starting the avatar, or start "
                "the avatar inside a LiveKit job"
            )
        self._livekit_url, self._livekit_api_key = livekit_url, livekit_api_key
        self._livekit_api_secret, self._room_name = livekit_api_secret, room_name
        self._agent_identity = local_participant_identity
        livekit_token = self._token(self._max_duration_seconds)

        logger.debug(
            "starting atmee avatar session",
            extra={"avatar_id": self._avatar_id, "lk.pii.room": room_name},
        )
        if self._closed:
            # closed before the render was requested: never open a billed render
            raise AtmeeException("the avatar session was closed before it started", code="closed")
        self._render_requested = True
        # The agent session can close without the job shutting down
        # (AgentSession.aclose(), an error), even while the start is still
        # waiting: end the render then too, or it would bill until
        # max_duration_seconds.
        self._agent_session_for_events = agent_session
        agent_session.on("close", self._on_agent_session_close)
        request = asyncio.ensure_future(
            self._api.create_avatar_session(
                self._avatar_id,
                livekit_url=livekit_url,
                livekit_token=livekit_token,
                agent_identity=local_participant_identity,
                max_duration_seconds=self._max_duration_seconds,
                metadata=self._metadata,
                wait_for=self._wait_for,
                stop=self._stop_start,
            )
        )
        self._start_request = request
        try:
            # waits without cancelling the request when start() itself is cancelled
            await asyncio.wait({request})
        except asyncio.CancelledError:
            # The caller gave up on start() (the job shuts down, a timeout of
            # its own): let the request finish and end what it opens.
            self._closed = True
            self._stop_start.set()
            task = asyncio.ensure_future(self._settle_closed_start(request))
            self._settle_tasks.add(task)
            task.add_done_callback(self._settle_tasks.discard)
            raise
        if self._closed:
            # aclose() ran while the request was waiting and left the teardown
            # to this start: end the session the API opened, if any, remove
            # the avatar and release the client, wiring nothing up.
            opened = await self._settle_closed_start(request)
            raise AtmeeException(
                "the avatar session was closed while it was starting"
                + ("; its render was ended" if opened else ""),
                code="closed",
            )
        info = request.result()
        self.session_info = info
        self.session_id = info.session_id
        logger.info(
            "atmee avatar session started",
            extra={
                "session_id": info.session_id,
                "status": info.status,
                "max_duration_seconds": info.max_duration_seconds,
            },
        )

        self._render_started_at = asyncio.get_running_loop().time()

        self._room_for_events = room
        room.on("participant_connected", self._on_participant_connected)
        room.on("participant_disconnected", self._on_participant_disconnected)
        room.on("data_received", self._on_data_received)
        self._renderer_sid = self._sid_in(room)  # with wait_for="avatar_joined", it is in
        self._watch_task = asyncio.create_task(self._watch_heartbeat())

        self._previous_audio = agent_session.output.audio
        self._previous_audio_tail = _last_sink(self._previous_audio)
        self._route_audio_to_avatar(agent_session, room)

    def _token(self, ttl_seconds: float) -> str:
        """The avatar participant's join token, minted with your LiveKit secret."""
        return (
            api.AccessToken(api_key=self._livekit_api_key, api_secret=self._livekit_api_secret)
            .with_kind("agent")
            .with_identity(self._avatar_participant_identity)
            .with_name(self._avatar_participant_name)
            # What the renderer needs and nothing more: it publishes video and
            # audio, sends its receipts over data, and subscribes to no one;
            # the agent's audio reaches it over a data stream.
            .with_grants(
                api.VideoGrants(
                    room_join=True,
                    room=self._room_name,
                    can_publish=True,
                    can_publish_data=True,
                    can_subscribe=False,
                )
            )
            # allow the avatar to publish audio and video on behalf of your local agent
            .with_attributes({ATTRIBUTE_PUBLISH_ON_BEHALF: self._agent_identity})
            .with_ttl(timedelta(seconds=ttl_seconds) + _TOKEN_TTL_SLACK)
            .to_jwt()
        )

    def _route_audio_to_avatar(self, agent_session: AgentSession[Any], room: rtc.Room) -> None:
        output = _AvatarAudioOutput(
            room=room,
            destination_identity=self._avatar_participant_identity,
            sample_rate=SAMPLE_RATE,
            wait_remote_track=rtc.TrackKind.KIND_VIDEO,
            wait_playback_start=True,
        )
        if self._audio_output is not None:
            # the renderer it spoke to is gone; nothing will answer for it
            self._audio_output.avatar_lost()
        self._audio_output, self._audio_agent_session = output, agent_session
        agent_session.output.replace_audio_tail(output)

    def _give_the_agent_its_voice_back(self) -> None:
        """The avatar is gone: stop waiting on it, and let the agent speak
        through what it spoke through before the avatar took over, if
        anything. Started before the agent session, the avatar replaced
        nothing: the agent then goes on without audio."""
        output, agent_session = self._audio_output, self._audio_agent_session
        if output is None or agent_session is None:
            return
        output.avatar_lost()
        audio = agent_session.output.audio
        if audio is output and self._previous_audio is not None:
            # the avatar's output replaced the whole chain
            agent_session.output.audio = self._previous_audio
        elif self._previous_audio_tail is not None and _last_sink(audio) is output:
            agent_session.output.replace_audio_tail(self._previous_audio_tail)
        else:
            return
        self._audio_output = self._audio_agent_session = None
        logger.info("the agent speaks with its own voice again")

    def _sid_in(self, room: rtc.Room) -> str | None:
        """The sid of the avatar participant in ``room``, if it is there."""
        participants = getattr(room, "remote_participants", None) or {}
        participant = participants.get(self._avatar_participant_identity)
        return getattr(participant, "sid", None) or None

    def _on_participant_connected(self, participant: rtc.RemoteParticipant) -> None:
        if participant.identity == self._avatar_participant_identity:
            self._renderer_sid = getattr(participant, "sid", None) or None

    def _on_participant_disconnected(self, participant: rtc.RemoteParticipant) -> None:
        if participant.identity != self._avatar_participant_identity or self._closed:
            return
        sid = getattr(participant, "sid", None)
        if sid and (
            sid in self._replaced_sids or (self._renderer_sid and sid != self._renderer_sid)
        ):
            return  # a predecessor its replacement evicted
        logger.warning(
            "atmee avatar participant left the room",
            extra={"session_id": self.session_id, "lk.pii.identity": participant.identity},
        )
        self.emit("avatar_disconnected", participant)
        self._renderer_lost("left the room")

    def _on_data_received(self, packet: rtc.DataPacket) -> None:
        sender = packet.participant
        if (
            packet.topic != _STATUS_TOPIC
            or sender is None
            or sender.identity != self._avatar_participant_identity
        ):
            return
        try:
            event = json.loads(packet.data).get("event")
        except (TypeError, ValueError, AttributeError):
            return
        if event == "avatar_worker_heartbeat":
            self._last_beat = asyncio.get_running_loop().time()
            self._beat_heard.set()
        elif event == "avatar_worker_going_away":
            self._renderer_lost(
                "going away", retiring=True, sid=getattr(sender, "sid", None) or None
            )

    async def _watch_heartbeat(self) -> None:
        """Take a renderer that stopped beating for gone. Armed only once it
        was heard from, so a renderer that sends no heartbeat is never taken
        for dead."""
        loop = asyncio.get_running_loop()
        while not self._closed:
            last = self._last_beat
            if last is None:
                self._beat_heard.clear()
                await self._beat_heard.wait()
                continue
            silent_for = loop.time() - last
            if silent_for < _HEARTBEAT_TIMEOUT:
                await asyncio.sleep(_HEARTBEAT_TIMEOUT - silent_for)
                continue
            self._last_beat = None  # armed again by the next renderer's first beat
            self._renderer_lost("stopped its heartbeat")

    def _renderer_lost(self, why: str, *, retiring: bool = False, sid: str | None = None) -> None:
        """The renderer is gone, or retiring: replace it, or go on without it."""
        if self._closed:
            return
        if not retiring:
            # its beats are over: the next renderer's first beat arms the watch
            self._last_beat = None
        replacing = self._replace_task is not None and not self._replace_task.done()
        if replacing:
            if not retiring and self._replacing_retiring:
                # the retiring renderer is gone before its replacement came
                self._give_the_agent_its_voice_back()
            return
        if not retiring:
            # nothing plays the agent's audio until a replacement is in
            self._give_the_agent_its_voice_back()
        if not self._may_replace():
            if not retiring:  # a retiring renderer plays on; its leaving comes later
                logger.warning(
                    "atmee avatar renderer lost; continuing without the avatar",
                    extra={"session_id": self.session_id, "reason": why},
                )
                self._schedule_end()
                self.emit("avatar_lost", None)
            return
        logger.warning(
            "atmee avatar renderer lost; starting a replacement",
            extra={"session_id": self.session_id, "reason": why},
        )
        self._replacing_retiring = retiring
        self._outgoing_sid = sid or self._renderer_sid
        agent_session, room = self._agent_session_for_events, self._room_for_events
        assert agent_session is not None and room is not None  # _may_replace checked
        self._replace_task = asyncio.create_task(
            self._replace(agent_session, room, retiring=retiring)
        )

    def _ceiling_left(self) -> float:
        elapsed = asyncio.get_running_loop().time() - self._render_started_at
        return self._max_duration_seconds - elapsed

    def _may_replace(self) -> bool:
        if self._agent_session_for_events is None or self._room_for_events is None:
            return False
        if self._ceiling_left() < _MIN_REPLACEMENT_SECONDS:
            return False
        now = asyncio.get_running_loop().time()
        while self._replacements and now - self._replacements[0] > _REPLACEMENT_WINDOW:
            self._replacements.popleft()
        return len(self._replacements) < _MAX_REPLACEMENTS

    async def _replace(
        self, agent_session: AgentSession[Any], room: rtc.Room, *, retiring: bool
    ) -> None:
        """Start a new render under the avatar's identity and move the
        agent's voice to it once it is in the room."""
        if self._closed:
            return
        previous_session_id = self.session_id
        if not retiring:
            # the dead render: stop billing it, and free the slot it holds
            await self._end_session()
        previous_ended = self._ended
        if self._closed:
            return
        ceiling = int(self._ceiling_left())
        self._replacements.append(asyncio.get_running_loop().time())
        request = asyncio.ensure_future(
            self._api.create_avatar_session(
                self._avatar_id,
                livekit_url=self._livekit_url,
                livekit_token=self._token(ceiling),
                agent_identity=self._agent_identity,
                max_duration_seconds=ceiling,
                metadata=self._metadata,
                # its voice moves over only once it is in the room
                wait_for="avatar_joined",
                stop=self._stop_start,
            )
        )
        self._start_request = request
        # never cancelled: the render it may open could not be ended otherwise
        await asyncio.wait({request})
        if self._closed:
            await self._settle_closed_start(request)
            return
        try:
            info = request.result()
        except Exception as e:
            logger.warning(
                "could not replace the lost atmee avatar; continuing without it",
                extra={"error": str(e)},
            )
            if not retiring:
                self.emit("avatar_lost", e)
            # a retiring renderer plays on, and its leaving asks again
            return
        self.session_info, self.session_id, self._ended = info, info.session_id, False
        if self._outgoing_sid:
            self._replaced_sids.add(self._outgoing_sid)
        joined = self._sid_in(room)
        if joined and joined not in self._replaced_sids:
            self._renderer_sid = joined
        elif self._renderer_sid in self._replaced_sids:
            self._renderer_sid = None  # learnt when its join is reported
        self._route_audio_to_avatar(agent_session, room)
        if previous_session_id and not previous_ended:
            # a retiring render (or a dead one whose end failed) is over now
            await self._end_session_quietly(previous_session_id)
        logger.info(
            "atmee avatar replaced",
            extra={"session_id": info.session_id, "previous_session_id": previous_session_id},
        )
        self.emit("avatar_replaced", info)

    async def _end_session_quietly(self, session_id: str) -> None:
        try:
            await self._api.end_avatar_session(session_id)
        except Exception as e:
            logger.warning(
                "failed to end a replaced atmee avatar session; the worker's own report or "
                "the session ceiling will settle it",
                extra={"session_id": session_id, "error": str(e)},
            )

    def _on_agent_session_close(self, _: Any) -> None:
        if self._aclose_task is None:
            self._aclose_task = asyncio.create_task(self.aclose())

    def _schedule_end(self) -> None:
        if self._end_task is None or self._end_task.done():
            self._end_task = asyncio.create_task(self._end_session())

    async def _settle_closed_start(self, request: asyncio.Future[AvatarSessionInfo]) -> bool:
        """Close everything once a start closed mid-request has its answer:
        end the session it opened, if any. Returns whether one was opened."""
        await asyncio.wait({request})
        info = None
        if not request.cancelled() and request.exception() is None:
            info = request.result()  # failed or stopped requests opened nothing
        if info is not None:
            self.session_id, self._ended = info.session_id, False
        async with self._close_lock:
            await self._aclose_locked()
        return info is not None

    async def _end_session(self) -> None:
        """Best-effort ``POST /v1/avatar_sessions/{id}/end``.

        Serialized, and marked done only once the API confirmed the end (the
        call is idempotent), so a failed or cancelled attempt can be retried
        by a later :meth:`aclose`.
        """
        async with self._end_lock:
            if self._ended or not self.session_id:
                return
            try:
                await self._api.end_avatar_session(self.session_id)
            except AtmeeException as e:
                if 400 <= e.status_code < 500 and e.status_code not in (408, 429):
                    # The session is unknown or already settled on the Atmee
                    # side: nothing left to end, retrying would not help.
                    self._ended = True
                logger.warning(
                    "failed to end atmee avatar session; the worker's own report or the "
                    "session ceiling will settle it",
                    extra={"session_id": self.session_id, "error": str(e)},
                )
                return
            except Exception as e:  # never let a billing hint break teardown
                logger.warning(
                    "failed to end atmee avatar session; the worker's own report or the "
                    "session ceiling will settle it",
                    extra={"session_id": self.session_id, "error": str(e)},
                )
                return
            self._ended = True
            logger.debug("atmee avatar session ended", extra={"session_id": self.session_id})

    async def _finish_end(self) -> None:
        # an end already under way (the avatar left on its own) finishes first,
        # so none is still running once the HTTP client is released
        if self._end_task is not None and not self._end_task.done():
            try:
                await self._end_task
            except Exception:
                pass
        await self._end_session()

    async def aclose(self) -> None:
        """End the Atmee session and remove the avatar from the room.

        Registered as a job shutdown callback by :meth:`start`; call it
        yourself when running outside a job. Safe to call more than once and
        concurrently (the agent session's ``close`` event also triggers it):
        calls are serialized, so the HTTP session is never closed while
        another call is still ending the render.
        """
        self._closed = True
        self._stop_start.set()
        async with self._close_lock:
            await self._aclose_locked()

    async def _aclose_locked(self) -> None:
        if self._room_for_events is not None:
            self._room_for_events.off("participant_connected", self._on_participant_connected)
            self._room_for_events.off("participant_disconnected", self._on_participant_disconnected)
            self._room_for_events.off("data_received", self._on_data_received)
            self._room_for_events = None
        if self._watch_task is not None:
            self._watch_task.cancel()
            self._watch_task = None
        if self._agent_session_for_events is not None:
            self._agent_session_for_events.off("close", self._on_agent_session_close)
            self._agent_session_for_events = None
        if self._audio_output is not None:
            # nothing plays the agent's audio once the avatar is removed
            self._audio_output.avatar_lost()
        if self._start_request is not None and not self._start_request.done():
            # A start is still waiting on its request, with no session to end
            # yet: once the request returns, the session it opened is ended and
            # everything here is closed (see _settle_closed_start).
            return
        # Remove the avatar participant and end the render side by side, so the
        # avatar leaves at once even when the end call is slow: at job shutdown
        # the process has only a few seconds.
        results = await asyncio.gather(super().aclose(), self._finish_end(), return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.warning("atmee avatar session cleanup step failed", exc_info=result)
        await self._api.aclose()


def _last_sink(audio: AudioOutput | None) -> AudioOutput | None:
    """The sink at the bottom of an audio output chain."""
    while audio is not None and audio.next_in_chain is not None:
        audio = audio.next_in_chain
    return audio
