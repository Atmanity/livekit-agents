"""Unit tests for the Atmee avatar plugin: the ``AtmeeAPI`` client against a local
scripted Atmee API, and the ``AvatarSession`` lifecycle in a faked LiveKit room."""

from __future__ import annotations

import asyncio
import io
import json
import zipfile
from collections import defaultdict, deque
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any

import aiohttp
import jwt
import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestServer

import livekit.plugins.atmee.api as atmee_api
import livekit.plugins.atmee.avatar as atmee_avatar
from livekit import api as lk_api, rtc
from livekit.agents import (
    APIConnectionError,
    APIConnectOptions,
    APIError,
    APIStatusError,
    Plugin,
)
from livekit.agents.voice.avatar import AvatarSession as BaseAvatarSession, DataStreamAudioOutput
from livekit.agents.voice.io import AgentOutput, AudioOutput, AudioOutputCapabilities
from livekit.plugins import atmee
from livekit.plugins.atmee import (
    AtmeeAPI,
    AtmeeAvatarBusyError,
    AtmeeAvatarNotReadyError,
    AtmeeAvatarWarmingError,
    AtmeeConnectionError,
    AtmeeException,
    AtmeeNoCapacityError,
    AtmeeStatusError,
)
from livekit.plugins.atmee.avatar import _AvatarAudioOutput

pytestmark = [pytest.mark.unit, pytest.mark.plugin("atmee")]

AVATAR_ID = "0f0e0d0c-0b0a-4908-8706-050403020100"
SESSION_ID = "11111111-2222-4333-8444-555555555555"
SESSION_ID2 = "66666666-7777-4888-8999-aaaaaaaaaaaa"
LIVEKIT_SECRET = "the-developers-secret-the-developers-secret"
START = "/v1/avatar_sessions"
SESSION = f"/v1/avatar_sessions/{SESSION_ID}"
END = f"{SESSION}/end"
FAST = APIConnectOptions(max_retry=1, retry_interval=0.0, timeout=5.0)
STARTED = {
    "sessionId": SESSION_ID,
    "status": "initializing",
    "avatarParticipantIdentity": "atmee-avatar-agent",
    "agentIdentity": "my-agent",
    "roomName": "dev-room-42",
    "maxDurationSeconds": 3600,
    "billingMode": "metered",
}


class FakeAtmee:
    """A scripted Atmee API: answers each (method, path) from a queue and records
    every request; ``session`` is an HTTP client for it."""

    def __init__(self) -> None:
        self.requests: list[SimpleNamespace] = []
        self.answers: dict[tuple[str, str], deque[dict[str, Any]]] = defaultdict(deque)
        self.session: aiohttp.ClientSession

    def script(
        self, method: str, path: str, status: int, payload: Any = None, *, times: int = 1, **kw: Any
    ) -> None:
        """Queue an answer; ``kw`` takes ``body`` (text), ``headers`` and ``delay``."""
        self.answers[(method, path)].extend([{"status": status, "payload": payload, **kw}] * times)

    def calls(self, method: str, path: str) -> list[SimpleNamespace]:
        return [r for r in self.requests if (r.method, r.path) == (method, path)]

    async def handle(self, request: web.Request) -> web.StreamResponse:
        call = SimpleNamespace(
            method=request.method, path=request.path, query=dict(request.query), json=None
        )
        call.headers = dict(request.headers)
        call.form = None
        if request.content_type == "application/json":
            call.json = await request.json()
        elif request.content_type == "multipart/form-data":
            form = await request.post()
            call.form = {
                k: (v.file.read() if isinstance(v, web.FileField) else v) for k, v in form.items()
            }
        self.requests.append(call)
        if not (queue := self.answers.get((request.method, request.path))):
            return web.json_response({"error": "unscripted"}, status=404)
        a = queue.popleft()
        await asyncio.sleep(a.get("delay", 0))
        if a["payload"] is not None:
            return web.json_response(a["payload"], status=a["status"], headers=a.get("headers"))
        return web.Response(status=a["status"], text=a.get("body", ""), headers=a.get("headers"))


@pytest.fixture(autouse=True)
def _fresh_engine_cache() -> None:
    atmee_api._ENGINE_CACHE.clear()


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATMEE_API_KEY", "sk_atmee_test")
    monkeypatch.setenv("LIVEKIT_URL", "wss://dev.livekit.cloud")
    monkeypatch.setenv("LIVEKIT_API_KEY", "APIdevkey")
    monkeypatch.setenv("LIVEKIT_API_SECRET", LIVEKIT_SECRET)


@pytest_asyncio.fixture
async def fake(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[FakeAtmee]:
    fa, app = FakeAtmee(), web.Application()
    app.router.add_route("*", "/{tail:.*}", fa.handle)
    async with TestServer(app) as server, aiohttp.ClientSession() as fa.session:
        monkeypatch.setenv("ATMEE_API_URL", str(server.make_url("")).rstrip("/"))
        yield fa


class FakeRoom(rtc.EventEmitter[str]):
    """Enough of rtc.Room for AvatarSession.start(), disconnected so nothing talks
    to a server."""

    name = "dev-room-42"
    local_participant = SimpleNamespace(identity="my-agent")

    def isconnected(self) -> bool:
        return False


class FakeOutput:
    """An agent session's output with no wrappers: replacing the tail replaces it all."""

    def __init__(self, audio: Any = None) -> None:
        self.audio = audio
        self.audio_tails: list[Any] = []

    def replace_audio_tail(self, sink: Any) -> None:
        self.audio_tails.append(sink)
        self.audio = sink


class FakeAgentSession(rtc.EventEmitter[str]):
    def __init__(self, output: Any = None) -> None:
        super().__init__()
        self.output = output if output is not None else FakeOutput()
        self.audio_tails: list[Any] = getattr(self.output, "audio_tails", [])


class Sink(AudioOutput):
    """A sink that plays nothing and reports nothing, like the room's own audio."""

    def __init__(self, label: str = "room", next_in_chain: AudioOutput | None = None) -> None:
        super().__init__(
            label=label,
            capabilities=AudioOutputCapabilities(pause=True),
            next_in_chain=next_in_chain,
        )

    async def capture_frame(self, frame: rtc.AudioFrame) -> None:
        await super().capture_frame(frame)

    def flush(self) -> None:
        super().flush()

    def clear_buffer(self) -> None:
        pass


def _frame(rate: int = 16000, seconds: float = 0.1) -> rtc.AudioFrame:
    samples = int(rate * seconds)
    return rtc.AudioFrame(bytes(2 * samples), rate, 1, samples)


def _frames_go_nowhere(monkeypatch: pytest.MonkeyPatch) -> None:
    """Avatar outputs send no frames: the base only counts them."""

    async def capture(self: Any, frame: rtc.AudioFrame) -> None:
        await AudioOutput.capture_frame(self, frame)
        self._pushed_duration += frame.duration

    monkeypatch.setattr(DataStreamAudioOutput, "capture_frame", capture)


def _output(monkeypatch: pytest.MonkeyPatch) -> _AvatarAudioOutput:
    _frames_go_nowhere(monkeypatch)
    return _AvatarAudioOutput(
        room=FakeRoom(),  # type: ignore[arg-type]
        destination_identity="atmee-avatar-agent",
        sample_rate=16000,
        wait_playback_start=True,
    )


def _receipt(**event: Any) -> SimpleNamespace:
    return SimpleNamespace(caller_identity="atmee-avatar-agent", payload=json.dumps(event))


def _avatar(**kwargs: Any) -> atmee.AvatarSession:
    kwargs.setdefault("conn_options", FAST)
    return atmee.AvatarSession(avatar_id=AVATAR_ID, **kwargs)


async def _start(api: AtmeeAPI, **kwargs: Any) -> atmee.AvatarSessionInfo:
    return await api.create_avatar_session(
        AVATAR_ID, livekit_url="wss://x", livekit_token="t", **kwargs
    )


def test_plugin_registered() -> None:
    assert "livekit.plugins.atmee" in [p.title for p in Plugin.registered_plugins]


# --- AtmeeAPI -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "code", "error_type"),
    [
        (503, "no_capacity", AtmeeNoCapacityError),
        (503, "avatar_version_unavailable", AtmeeStatusError),
        (502, "avatar_start_failed", AtmeeStatusError),
        (504, "avatar_join_timeout", AtmeeStatusError),
        (409, "avatar_not_renderable", AtmeeAvatarNotReadyError),
        (409, "avatar_busy", AtmeeAvatarBusyError),
        (400, "missing_publish_on_behalf", AtmeeStatusError),
    ],
)
async def test_start_failures_are_typed_and_never_retried(
    fake: FakeAtmee, status: int, code: str, error_type: type[AtmeeException]
) -> None:
    # The POST bills a session once the server accepts it; a retry after a
    # timeout or 5xx could open a second render, so it is final either way.
    error = {"error": code, "message": "why"}
    fake.script("POST", START, status, error, headers={"Retry-After": "5"}, times=2)
    with pytest.raises(error_type) as exc:
        await _start(AtmeeAPI(session=fake.session, conn_options=FAST))
    assert type(exc.value) is error_type and (exc.value.status_code, exc.value.code) == (
        status,
        code,
    )
    assert "why" in str(exc.value) and len(fake.calls("POST", START)) == 1
    assert getattr(exc.value, "retry_after", 5.0) == 5.0
    # what other LiveKit plugins raise, retryable only where waiting can help
    assert isinstance(exc.value, APIStatusError) and exc.value.body == error
    assert exc.value.retryable == (code == "no_capacity")


async def test_a_busy_avatar_names_its_live_session(fake: FakeAtmee) -> None:
    busy = {"error": "avatar_busy", "message": "in use", "liveSessionId": SESSION_ID}
    fake.script("POST", START, 409, busy)
    with pytest.raises(AtmeeAvatarBusyError) as exc:
        await _start(AtmeeAPI(session=fake.session, conn_options=FAST))
    assert exc.value.live_session_id == SESSION_ID and not exc.value.retryable


async def test_a_final_5xx_is_not_retried(fake: FakeAtmee) -> None:
    fake.script("POST", END, 503, {"error": "embed_disabled"}, times=2)
    with pytest.raises(AtmeeStatusError) as exc:
        await AtmeeAPI(session=fake.session, conn_options=FAST).end_avatar_session(SESSION_ID)
    assert exc.value.code == "embed_disabled" and len(fake.calls("POST", END)) == 1


async def test_the_avatars_engine_is_read_once(
    fake: FakeAtmee, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake.script("GET", f"/v1/avatars/{AVATAR_ID}", 200, {"id": AVATAR_ID, "engine": "v2"})
    fake.script("POST", START, 202, STARTED, times=2)
    api = AtmeeAPI(session=fake.session, conn_options=FAST)
    await _start(api)
    await _start(api)
    assert len(fake.calls("GET", f"/v1/avatars/{AVATAR_ID}")) == 1
    assert len(fake.calls("POST", START)) == 2

    # an engine that could not be read is not remembered, and entries expire
    monkeypatch.setattr(atmee_api, "_ENGINE_CACHE_TTL", 0.0)
    fake.script("POST", START, 202, STARTED)
    await _start(api)
    assert len(fake.calls("GET", f"/v1/avatars/{AVATAR_ID}")) == 2


async def test_a_warming_avatar_is_asked_again_within_the_start_budget(
    fake: FakeAtmee, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A v2 avatar after a quiet period: nothing is reserved until it starts.
    monkeypatch.setattr(atmee_api, "_WARMING_RETRY_BOUNDS", (0.01, 0.01))
    for code in ("avatar_warming", "avatar_start_interrupted"):
        fake.script("POST", START, 503, {"error": code}, headers={"Retry-After": "9"})
    fake.script("POST", START, 202, STARTED)
    api = AtmeeAPI(session=fake.session, conn_options=FAST)
    assert (await _start(api)).session_id == SESSION_ID and len(fake.calls("POST", START)) == 3

    # once the next ask would land past the budget, the warming error is raised
    monkeypatch.setattr(atmee_api, "_WARMING_RETRY_BOUNDS", (1.0, 30.0))
    monkeypatch.setattr(
        atmee_api, "_START_TOTAL_TIMEOUT", {g: {"initializing": 0.05} for g in ("v1", "v2")}
    )
    fake.script("POST", START, 503, {"error": "avatar_warming"}, headers={"Retry-After": "9"})
    with pytest.raises(AtmeeAvatarWarmingError) as exc:
        await _start(api)
    assert (exc.value.code, exc.value.retry_after) == ("avatar_warming", 9.0)
    assert len(fake.calls("POST", START)) == 4


@pytest.mark.parametrize(
    ("avatar", "asks"),
    [
        ((200, {"avatarId": AVATAR_ID, "status": "ready", "engine": "v1"}), 1),
        ((200, {"avatarId": AVATAR_ID, "status": "ready", "engine": "v2hd"}), 3),
        ((403, {"error": "forbidden"}), 3),  # unreadable: allow for a v2 start
    ],
)
async def test_the_start_budget_follows_the_avatars_engine(
    fake: FakeAtmee, monkeypatch: pytest.MonkeyPatch, avatar: tuple[int, Any], asks: int
) -> None:
    # A v1 start that is not answered in time gives up after the v1 budget,
    # not the minutes a v2 avatar's first start may need.
    monkeypatch.setattr(atmee_api, "_WARMING_RETRY_BOUNDS", (0.05, 0.05))
    budgets = {"v1": {"initializing": 0.01}, "v2": {"initializing": 5.0}}
    monkeypatch.setattr(atmee_api, "_START_TOTAL_TIMEOUT", budgets)
    fake.script("GET", f"/v1/avatars/{AVATAR_ID}", *avatar)
    fake.script("POST", START, 503, {"error": "avatar_warming"}, times=2)
    fake.script("POST", START, 202, STARTED)
    api = AtmeeAPI(session=fake.session, conn_options=FAST)
    if asks == 1:
        with pytest.raises(AtmeeAvatarWarmingError):
            await _start(api)
    else:
        assert (await _start(api)).session_id == SESSION_ID
    assert len(fake.calls("POST", START)) == asks


@pytest.mark.parametrize(
    ("answers", "error"),
    [
        ([(502, "bad gateway")], None),  # retried, then the 200
        ([(503, "Service Unavailable")], None),  # a plain 503, e.g. from a proxy
        ([(500, "down")] * 2, 500),  # max_retry=1: two attempts, then the 5xx
        ([(200, "<html>not json</html>")], "invalid_response"),  # a 2xx is never retried
    ],
)
async def test_idempotent_calls_retry_5xx(
    fake: FakeAtmee, answers: list[tuple[int, str]], error: int | str | None
) -> None:
    for status, text in answers:
        fake.script("GET", SESSION, status, body=text)
    fake.script("GET", SESSION, 200, {"status": "active"})
    api = AtmeeAPI(session=fake.session, conn_options=FAST)
    if error is None:
        assert (await api.get_avatar_session(SESSION_ID))["status"] == "active"
    else:
        with pytest.raises(AtmeeException) as exc:
            await api.get_avatar_session(SESSION_ID)
        assert error in (exc.value.status_code, exc.value.code)
    assert len(fake.calls("GET", SESSION)) == len(answers) + (error is None)


async def test_idempotent_calls_are_bounded_by_conn_options(fake: FakeAtmee) -> None:
    fake.script("POST", END, 200, {}, delay=1.0)
    api = AtmeeAPI(session=fake.session, conn_options=APIConnectOptions(max_retry=0, timeout=0.1))
    began = asyncio.get_running_loop().time()
    with pytest.raises(AtmeeConnectionError) as exc:
        await api.end_avatar_session(SESSION_ID)
    assert exc.value.status_code == 0 and asyncio.get_running_loop().time() - began < 0.9
    assert isinstance(exc.value, APIConnectionError) and exc.value.retryable


@pytest.mark.parametrize(
    ("reported", "engine", "ready"),
    [
        ({"status": "ready"}, None, True),
        ({"status": "ready", "engine": "v1"}, "v1", True),
        ({"status": "ready", "engine": "v2"}, "v2", False),  # no take served yet
        ({"status": "ready", "engine": "v2", "servingCandidateId": "c1"}, "v2", True),
        ({"status": "ready", "engine": "v2hd", "servingCandidateId": "c1"}, "v2hd", True),
        ({"status": "building", "engine": "v2hd"}, "v2hd", False),
    ],
)
def test_avatar_info_reports_the_engine(
    reported: dict[str, Any], engine: str | None, ready: bool
) -> None:
    info = atmee_api._avatar_info({"id": AVATAR_ID, **reported})
    assert (info.avatar_id, info.engine, info.ready) == (AVATAR_ID, engine, ready)


def test_client_guards(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(AtmeeException, match="https"):
        AtmeeAPI(api_url="http://api.example.com")
    AtmeeAPI(api_url="http://127.0.0.1:8080")  # loopback is fine for local development
    assert str(AtmeeException("nope", status_code=402, code="x")) == "nope [x] (HTTP 402)"
    assert str(AtmeeStatusError("nope", status_code=402, code="x")) == "nope [x] (HTTP 402)"
    assert isinstance(AtmeeException("nope"), APIError)
    monkeypatch.delenv("ATMEE_API_KEY")
    with pytest.raises(AtmeeException):
        AtmeeAPI()


# --- AvatarSession --------------------------------------------------------------


async def test_start_mints_token_posts_session_and_routes_audio(fake: FakeAtmee) -> None:
    fake.script("POST", START, 202, STARTED)
    fake.script("POST", END, 200, {"status": "completed"})
    agent_session = FakeAgentSession()
    avatar = _avatar(metadata={"k": "v"})  # outside a job the client owns its HTTP session
    assert (avatar.provider, avatar.avatar_identity) == ("atmee", "atmee-avatar-agent")
    await avatar.start(agent_session, FakeRoom())  # type: ignore[arg-type]

    call = fake.calls("POST", START)[0]
    token = call.json.pop("livekitToken")
    assert call.headers["X-Api-Key"] == "sk_atmee_test" and call.query == {}
    # the request names no avatar generation: the avatar itself decides
    assert call.json == {
        "avatarId": AVATAR_ID,
        "livekitUrl": "wss://dev.livekit.cloud",
        "agentIdentity": "my-agent",
        "maxDurationSeconds": 3600,
        "metadata": {"k": "v"},
    }
    # The token is minted with the developer's own LiveKit credentials, for the
    # avatar participant, granting exactly this room, publishing on behalf of
    # the local agent.
    claims = lk_api.TokenVerifier("APIdevkey", LIVEKIT_SECRET).verify(token)
    assert claims.identity == "atmee-avatar-agent"
    assert claims.attributes == {"lk.publish_on_behalf": "my-agent"}
    assert (
        claims.video is not None and claims.video.room == "dev-room-42" and claims.video.room_join
    )
    # and nothing more: it publishes and sends receipts, and watches no one
    assert claims.video.can_publish and claims.video.can_publish_data
    assert claims.video.can_subscribe is False
    assert jwt.decode(token, options={"verify_signature": False})["kind"] == "agent"

    info = avatar.session_info
    assert info is not None and avatar.session_id == info.session_id == SESSION_ID
    assert info.status == "initializing" and info.max_duration_seconds == 3600
    [tail] = agent_session.audio_tails
    assert isinstance(tail, DataStreamAudioOutput) and tail.sample_rate == 16000
    assert tail._destination_identity == "atmee-avatar-agent"

    await avatar.aclose()
    await avatar.aclose()  # idempotent locally too
    assert len(fake.calls("POST", END)) == 1 and avatar._api._session is None


async def test_bad_arguments_are_refused_before_anything_happens(fake: FakeAtmee) -> None:
    with pytest.raises(ValueError, match="wait_for"):
        _avatar(wait_for="joined")
    with pytest.raises(ValueError, match="wait_for"):
        await _start(AtmeeAPI(session=fake.session), wait_for="joined")
    with pytest.raises(AtmeeException):
        atmee.AvatarSession(avatar_id="")
    for ceiling in (0, -60):
        with pytest.raises(ValueError, match="max_duration_seconds"):
            _avatar(max_duration_seconds=ceiling)
    closed = _avatar(http_session=fake.session)
    await closed.aclose()  # closed before it started: no billed render is opened
    with pytest.raises(AtmeeException, match="closed"):
        await closed.start(FakeAgentSession(), FakeRoom())  # type: ignore[arg-type]
    assert fake.requests == []


async def test_avatar_version_is_accepted_but_ignored(
    fake: FakeAtmee, caplog: pytest.LogCaptureFixture
) -> None:
    # code written for releases that took avatar_version keeps working
    fake.script("POST", START, 202, STARTED, times=2)
    avatar = _avatar(http_session=fake.session, avatar_version="v2")
    await avatar.start(FakeAgentSession(), FakeRoom())  # type: ignore[arg-type]
    api = AtmeeAPI(session=fake.session, conn_options=FAST)
    await _start(api, avatar_version="v1")
    await avatar.aclose()
    assert [r.message for r in caplog.records].count(
        "avatar_version is deprecated and ignored, the avatar's own engine decides how it renders"
    ) == 2
    assert all("avatarVersion" not in (c.json or {}) for c in fake.requests)
    assert atmee.SUPPORTED_AVATAR_VERSIONS == {"v1", "v2"} and atmee.AvatarVersion


@pytest.mark.parametrize("image", ["https://cdn.example/v.jpg", b"jpeg"])
async def test_create_a_v1_avatar_from_a_portrait(fake: FakeAtmee, image: str | bytes) -> None:
    fake.script("POST", "/v1/avatars", 201, {"avatarId": AVATAR_ID, "status": "ready"})
    api = AtmeeAPI(session=fake.session, conn_options=FAST)
    assert await api.create_avatar("Val", image) == AVATAR_ID
    call = fake.calls("POST", "/v1/avatars")[0]
    if isinstance(image, str):
        assert call.json == {"schemaVersion": 1, "name": "Val", "assets": {"image": {"url": image}}}
    else:
        assert call.form == {"name": "Val", "file": b"jpeg"}


async def test_create_a_v2_avatar_from_urls(fake: FakeAtmee) -> None:
    fake.script("POST", "/v1/avatars", 202, {"avatarId": AVATAR_ID, "status": "building"})
    api = AtmeeAPI(session=fake.session, conn_options=FAST)
    image, voice = "https://cdn.example/val.png", "https://cdn.example/val.mp3"
    assert await api.create_avatar("Val", image, voice=voice, engine="v2") == AVATAR_ID
    assert fake.calls("POST", "/v1/avatars")[0].json == {
        "schemaVersion": 1,
        "name": "Val",
        "engine": "v2",
        "assets": {"image": {"url": image}, "voice": {"url": voice}},
    }


async def test_create_a_v2_avatar_from_files(fake: FakeAtmee, tmp_path: Any) -> None:
    # local files, and a URL among them, go as one bundle
    (tmp_path / "val.png").write_bytes(b"png")
    fake.script("GET", "/files/sample", 200, body="mp3", headers={"Content-Type": "audio/mpeg"})
    fake.script("POST", "/v1/avatars", 202, {"avatarId": AVATAR_ID, "status": "building"})
    api = AtmeeAPI(session=fake.session, conn_options=FAST)
    voice = f"{api._api_url}/files/sample"
    created = await api.create_avatar(
        "Val", tmp_path / "val.png", voice=voice, engine="v2hd", description="d"
    )
    assert created == AVATAR_ID
    assert "X-Api-Key" not in fake.calls("GET", "/files/sample")[0].headers  # not leaked
    bundle = zipfile.ZipFile(io.BytesIO(fake.calls("POST", "/v1/avatars")[0].form["file"]))
    assert sorted(bundle.namelist()) == ["avatar.json", "image/val.png", "voice/sample.mp3"]
    assert json.loads(bundle.read("avatar.json")) == {
        "schemaVersion": 1,
        "name": "Val",
        "description": "d",
        "engine": "v2hd",
    }
    assert bundle.read("voice/sample.mp3") == b"mp3"


async def test_create_avatar_refuses_what_cannot_build(
    fake: FakeAtmee, caplog: pytest.LogCaptureFixture
) -> None:
    api = AtmeeAPI(session=fake.session, conn_options=FAST)
    with pytest.raises(ValueError, match="voice"):
        await api.create_avatar("Val", b"jpeg", engine="v2")
    with pytest.raises(ValueError, match="engine"):
        await api.create_avatar("Val", b"jpeg", engine="v3")  # type: ignore[arg-type]
    # the older avatar_version still asks for an engine, which it never did
    with pytest.raises(ValueError, match="voice"):
        await api.create_avatar("Val", b"jpeg", avatar_version="v2")
    assert "avatar_version is deprecated" in caplog.text and fake.requests == []


async def test_wait_until_ready(fake: FakeAtmee) -> None:
    path = f"/v1/avatars/{AVATAR_ID}"
    fake.script("GET", path, 200, {"id": AVATAR_ID, "status": "building", "engine": "v2"})
    fake.script("GET", path, 200, {"id": AVATAR_ID, "status": "ready", "readyToChat": True})
    api = AtmeeAPI(session=fake.session, conn_options=FAST)
    assert (await api.wait_until_ready(AVATAR_ID, poll_interval=0.01)).ready

    fake.script("GET", path, 200, {"id": AVATAR_ID, "status": "awaiting_selection"})
    with pytest.raises(AtmeeAvatarNotReadyError) as exc:
        await api.wait_until_ready(AVATAR_ID, poll_interval=0.01)
    assert exc.value.code == "awaiting_selection"

    fake.script("GET", path, 200, {"id": AVATAR_ID, "status": "building"}, times=3)
    with pytest.raises(AtmeeConnectionError, match="still building"):
        await api.wait_until_ready(AVATAR_ID, timeout=0.02, poll_interval=0.01)


async def test_a_reply_in_progress_reaches_the_avatar_at_16k(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # the avatar took over mid-reply: the agent still sends the TTS's own rate
    rates: list[int] = []

    async def capture(self: Any, frame: rtc.AudioFrame) -> None:
        rates.append(frame.sample_rate)

    monkeypatch.setattr(DataStreamAudioOutput, "capture_frame", capture)
    out = _AvatarAudioOutput(room=FakeRoom(), destination_identity="a", sample_rate=16000)  # type: ignore[arg-type]
    for rate in (24000, 24000, 24000, 16000):
        await out.capture_frame(rtc.AudioFrame(bytes(rate // 5), rate, 1, rate // 10))
    assert rates and set(rates) == {16000}


@pytest.mark.parametrize("trigger", ["avatar_left", "agent_session_closed"])
async def test_the_render_ends_once(fake: FakeAtmee, trigger: str) -> None:
    fake.script("POST", START, 202, STARTED)
    fake.script("POST", END, 200, {"status": "completed"})
    room, agent_session, avatar = FakeRoom(), FakeAgentSession(), _avatar()
    left: list[str] = []
    avatar.on("avatar_disconnected", lambda p: left.append(p.identity))
    await avatar.start(agent_session, room)  # type: ignore[arg-type]
    if trigger == "avatar_left":
        for identity in ("someone-else", "atmee-avatar-agent"):
            room.emit("participant_disconnected", SimpleNamespace(identity=identity))
        assert left == ["atmee-avatar-agent"]
    else:
        agent_session.emit("close", None)  # AgentSession.aclose() without a job shutdown
    await avatar.aclose()  # overlaps the end (or close) that was just triggered
    for _ in range(10):
        await asyncio.sleep(0)
    assert len(fake.calls("POST", END)) == 1 and avatar._ended and avatar._api._session is None
    # our listeners are gone (the agent's audio output keeps its own)
    assert not room._events["participant_disconnected"] and not agent_session._events["close"]


@pytest.mark.parametrize(("failure", "ends"), [("5xx", 3), ("4xx", 1)])
async def test_a_failed_end_never_breaks_close(fake: FakeAtmee, failure: str, ends: int) -> None:
    fake.script("POST", START, 202, STARTED)
    if failure == "5xx":
        fake.script("POST", END, 500, body="down", times=2)  # max_retry=1: two attempts
    else:
        fake.script("POST", END, 404, {"error": "not_found"})
    fake.script("POST", END, 200, {})
    avatar = _avatar(http_session=fake.session)
    await avatar.start(FakeAgentSession(), FakeRoom())  # type: ignore[arg-type]
    for _ in range(3):
        await avatar.aclose()  # never raises
    # the next aclose() tries a failed end again; a 4xx means nothing is left to end
    assert len(fake.calls("POST", END)) == ends


async def test_close_removes_the_avatar_without_waiting_for_the_end(
    fake: FakeAtmee, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake.script("POST", START, 202, STARTED)
    fake.script("POST", END, 200, {}, delay=0.5)  # a slow end call
    avatar = _avatar(http_session=fake.session)
    await avatar.start(FakeAgentSession(), FakeRoom())  # type: ignore[arg-type]
    removed, base_aclose = asyncio.Event(), BaseAvatarSession.aclose

    async def removing_aclose(self: Any) -> None:  # the base aclose() removes the participant
        removed.set()
        await base_aclose(self)

    monkeypatch.setattr(BaseAvatarSession, "aclose", removing_aclose)
    closing = asyncio.create_task(avatar.aclose())
    await asyncio.wait_for(removed.wait(), timeout=0.3)
    assert not closing.done()  # removed while the end is still in flight
    await closing
    assert avatar._ended


@pytest.mark.parametrize("trigger", ["aclose", "agent_session_closed", "start_cancelled"])
async def test_close_during_a_pending_start_ends_the_late_render(
    fake: FakeAtmee, trigger: str
) -> None:
    # The start is still waiting (a v2 avatar's slow first start) when the job
    # shuts down, the agent session closes, or the caller gives up on start().
    # The request is never cut off: the render it opens could not be ended.
    fake.script("POST", START, 202, STARTED, delay=0.3)
    fake.script("POST", END, 200, {})
    room, agent_session = FakeRoom(), FakeAgentSession()
    avatar = _avatar()  # owns its HTTP session: closing it early would abort the request
    starting = asyncio.create_task(avatar.start(agent_session, room))  # type: ignore[arg-type]
    while not fake.calls("POST", START):
        await asyncio.sleep(0.01)
    if trigger == "start_cancelled":
        starting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await starting
        await asyncio.wait_for(asyncio.gather(*avatar._settle_tasks), timeout=1)
    else:
        if trigger == "aclose":
            await asyncio.wait_for(avatar.aclose(), timeout=0.2)  # never waits for the start
        else:
            agent_session.emit("close", None)
        with pytest.raises(AtmeeException) as exc:
            await starting
        assert exc.value.code == "closed"
    assert len(fake.calls("POST", END)) == 1  # the late render ended
    assert agent_session.audio_tails == [] and avatar._api._session is None  # nothing wired up
    assert not any(agent_session._events.values()) and not any(room._events.values())


async def test_close_stops_asking_a_warming_avatar_again(fake: FakeAtmee) -> None:
    # Nothing is reserved between warming answers: a close ends the wait at once
    # and never sends the request that would open a render.
    fake.script("POST", START, 503, {"error": "avatar_warming"}, headers={"Retry-After": "30"})
    fake.script("POST", START, 202, STARTED)
    avatar = _avatar(http_session=fake.session)
    starting = asyncio.create_task(avatar.start(FakeAgentSession(), FakeRoom()))  # type: ignore[arg-type]
    while not fake.calls("POST", START):
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.05)  # now waiting out the Retry-After
    await asyncio.wait_for(avatar.aclose(), timeout=0.2)
    with pytest.raises(AtmeeException) as exc:
        await asyncio.wait_for(starting, timeout=1)
    assert exc.value.code == "closed"
    assert len(fake.calls("POST", START)) == 1 and fake.calls("POST", END) == []


async def test_start_forwards_its_options_and_is_one_shot(fake: FakeAtmee) -> None:
    fake.script("POST", START, 202, {**STARTED, "status": "avatar_joined"})
    fake.script("POST", START, 202, {"status": "initializing"})
    avatar = _avatar(
        http_session=fake.session,
        wait_for="avatar_joined",
        max_duration_seconds=600,
        avatar_participant_identity="val",
    )
    await avatar.start(FakeAgentSession(), FakeRoom())  # type: ignore[arg-type]
    call = fake.calls("POST", START)[0]
    assert call.query == {"waitFor": "avatar_joined"} and call.json["maxDurationSeconds"] == 600
    claims = lk_api.TokenVerifier("APIdevkey", LIVEKIT_SECRET).verify(call.json["livekitToken"])
    assert claims.identity == "val" == avatar.avatar_identity
    assert avatar.session_info is not None and avatar.session_info.status == "avatar_joined"
    with pytest.raises(AtmeeException, match="already called"):
        await avatar.start(FakeAgentSession(), FakeRoom())  # type: ignore[arg-type]
    assert len(fake.calls("POST", START)) == 1  # no second billed render
    await avatar.aclose()

    avatar = _avatar(http_session=fake.session)  # a 202 without a session id is refused
    with pytest.raises(AtmeeException) as exc:
        await avatar.start(FakeAgentSession(), FakeRoom())  # type: ignore[arg-type]
    assert exc.value.code == "invalid_response" and avatar.session_id is None


async def test_a_failed_start_cleans_up(fake: FakeAtmee, monkeypatch: pytest.MonkeyPatch) -> None:
    rooms_seen: list[Any] = []  # the room the base aclose() would remove the avatar from
    base_aclose = BaseAvatarSession.aclose

    async def recording_aclose(self: Any) -> None:
        rooms_seen.append(self._room)
        await base_aclose(self)

    monkeypatch.setattr(BaseAvatarSession, "aclose", recording_aclose)

    # 1) fails before Atmee is contacted: no avatar of ours can be in the room
    monkeypatch.delenv("LIVEKIT_API_SECRET")
    room, avatar = FakeRoom(), _avatar(http_session=fake.session)
    with pytest.raises(AtmeeException):
        await avatar.start(FakeAgentSession(), room)  # type: ignore[arg-type]
    assert rooms_seen == [None] and not any(room._events.values()) and fake.requests == []

    # 2) the render request failed: close fully, undo every hook, stay spent
    monkeypatch.setenv("LIVEKIT_API_SECRET", LIVEKIT_SECRET)
    fake.script("POST", START, 503, {"error": "no_capacity"}, times=2)
    room, agent_session, avatar = FakeRoom(), FakeAgentSession(), _avatar(http_session=fake.session)
    with pytest.raises(AtmeeNoCapacityError):
        await avatar.start(agent_session, room)  # type: ignore[arg-type]
    assert rooms_seen[-1] is room and not fake.calls("POST", END)
    assert not any(agent_session._events.values()) and not any(room._events.values())
    with pytest.raises(AtmeeException, match="already called"):
        await avatar.start(agent_session, room)  # type: ignore[arg-type]

    # 3) a teardown error never hides the real start failure
    avatar = _avatar(http_session=fake.session)

    async def broken_close() -> None:
        raise RuntimeError("teardown broke")

    monkeypatch.setattr(avatar._api, "aclose", broken_close)
    with pytest.raises(AtmeeNoCapacityError):
        await avatar.start(FakeAgentSession(), FakeRoom())  # type: ignore[arg-type]


async def test_start_names_the_room_from_the_job_before_it_connects(
    fake: FakeAtmee, monkeypatch: pytest.MonkeyPatch
) -> None:
    # a v2 avatar's slow start can overlap the agent connecting to its room
    class UnconnectedRoom(FakeRoom):
        name = ""

    fake.script("POST", START, 202, STARTED)
    avatar = _avatar(http_session=fake.session)
    with pytest.raises(AtmeeException, match="no name"):  # outside a job: nothing to go by
        await avatar.start(FakeAgentSession(), UnconnectedRoom())  # type: ignore[arg-type]
    assert fake.requests == []

    job = SimpleNamespace(
        local_participant_identity="my-agent", job=SimpleNamespace(room=SimpleNamespace(name="r9"))
    )
    monkeypatch.setattr(atmee_avatar, "get_job_context", lambda required=False: job)
    avatar = _avatar(http_session=fake.session)
    await avatar.start(FakeAgentSession(), UnconnectedRoom())  # type: ignore[arg-type]
    token = fake.calls("POST", START)[0].json["livekitToken"]
    claims = lk_api.TokenVerifier("APIdevkey", LIVEKIT_SECRET).verify(token)
    assert claims.video is not None and claims.video.room == "r9"
    await avatar.aclose()


async def test_the_resampler_belongs_to_one_segment(monkeypatch: pytest.MonkeyPatch) -> None:
    rates: list[int] = []
    out = _output(monkeypatch)
    sent = DataStreamAudioOutput.capture_frame

    async def capture(self: Any, frame: rtc.AudioFrame) -> None:
        rates.append(frame.sample_rate)
        await sent(self, frame)

    monkeypatch.setattr(DataStreamAudioOutput, "capture_frame", capture)
    await out.capture_frame(_frame(24000))
    out.flush()  # what it still holds is dropped, not carried into the next reply
    assert out._resampler is None
    await out.capture_frame(_frame(48000))
    assert out._resampler is not None and out._resampler_input_rate == 48000
    await out.capture_frame(_frame(24000))  # a new rate mid-segment gets a resampler of its own
    assert out._resampler_input_rate == 24000
    out.clear_buffer()
    assert out._resampler is None and set(rates) == {16000}


async def test_a_lost_avatar_is_never_waited_on(monkeypatch: pytest.MonkeyPatch) -> None:
    out = _output(monkeypatch)
    finished: list[Any] = []
    out.on("playback_finished", finished.append)
    # a reply the avatar started playing, flushed, its receipt still to come
    await out.capture_frame(_frame(seconds=2.0))
    out._handle_playback_started(_receipt())
    out.flush()
    # and the next, still being captured
    await out.capture_frame(_frame())
    out.avatar_lost()
    # the flushed reply ends at once, interrupted where the avatar must have
    # been: it only just started, far short of the 2 s it was given
    [first] = finished
    assert first.interrupted and 0.0 <= first.playback_position < 1.0

    await out.capture_frame(_frame())
    out.flush()  # the reply it was cut off in ends as interrupted
    second = await asyncio.wait_for(out.wait_for_playout(), 0.1)
    assert second.interrupted and second.playback_position == 0.0

    # later replies play nowhere: they count as played, a clear as an interruption
    started: list[float] = []
    out.on("playback_started", lambda ev: started.append(ev.created_at))
    await out.capture_frame(_frame(seconds=0.5))
    out.flush()
    third = await asyncio.wait_for(out.wait_for_playout(), 0.1)
    assert not third.interrupted and third.playback_position == pytest.approx(0.5)
    await out.capture_frame(_frame())
    out.clear_buffer()
    out.flush()
    assert (await asyncio.wait_for(out.wait_for_playout(), 0.1)).interrupted
    assert len(started) == 2 and len(finished) == 4

    # a late receipt from the avatar that left changes nothing
    assert out._handle_playback_finished(_receipt(playback_position=9.0, interrupted=False)) == "ok"
    assert out._pending_playback_count == 0 and len(finished) == 4


async def _lose_the_avatar(fake: FakeAtmee, agent_session: FakeAgentSession) -> Any:
    fake.script("POST", START, 202, STARTED)
    fake.script("POST", END, 200, {})
    room, avatar = FakeRoom(), _avatar(http_session=fake.session, replace_lost_avatar=False)
    await avatar.start(agent_session, room)  # type: ignore[arg-type]
    output = avatar._audio_output
    assert isinstance(output, _AvatarAudioOutput)
    room.emit("participant_disconnected", SimpleNamespace(identity="atmee-avatar-agent"))
    await avatar.aclose()
    return output


async def test_the_agent_gets_its_voice_back_when_the_avatar_is_lost(fake: FakeAtmee) -> None:
    # The avatar took over a running agent's voice, under the agent session's
    # wrappers: the room's sink is back under them.
    room_sink = Sink()
    output = AgentOutput(lambda: None, lambda: None, lambda: None)
    output.audio = Sink("wrapper", next_in_chain=room_sink)
    wrapper = output.audio
    lost = await _lose_the_avatar(fake, FakeAgentSession(output))
    assert lost._lost and output.audio is wrapper
    assert atmee_avatar._last_sink(output.audio) is room_sink

    # no wrappers: the avatar replaced the whole output, which comes back whole
    before = Sink()
    agent_session = FakeAgentSession(FakeOutput(before))
    await _lose_the_avatar(fake, agent_session)
    assert agent_session.output.audio is before


async def test_an_avatar_started_first_leaves_the_agent_without_audio(fake: FakeAtmee) -> None:
    agent_session = FakeAgentSession()
    lost = await _lose_the_avatar(fake, agent_session)
    assert lost._lost and agent_session.output.audio is lost  # nothing to give back


async def test_close_stops_the_agent_waiting_on_the_avatar(
    fake: FakeAtmee, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake.script("POST", START, 202, STARTED)
    fake.script("POST", END, 200, {})
    avatar = _avatar(http_session=fake.session)
    await avatar.start(FakeAgentSession(), FakeRoom())  # type: ignore[arg-type]
    output = avatar._audio_output
    assert output is not None
    _frames_go_nowhere(monkeypatch)
    await output.capture_frame(_frame())
    output.flush()
    await avatar.aclose()
    assert (await asyncio.wait_for(output.wait_for_playout(), 0.1)).interrupted


# --- replacing a lost renderer -----------------------------------------------------

END2 = f"/v1/avatar_sessions/{SESSION_ID2}/end"
STARTED2 = {**STARTED, "sessionId": SESSION_ID2, "status": "avatar_joined"}


def _renderer(sid: str) -> SimpleNamespace:
    return SimpleNamespace(identity="atmee-avatar-agent", sid=sid)


def _status(event: str, sid: str = "PA_1") -> SimpleNamespace:
    data = json.dumps({"event": event}).encode()
    return SimpleNamespace(topic="atmanity-avatar_status", participant=_renderer(sid), data=data)


async def _started_avatar(
    fake: FakeAtmee, **kwargs: Any
) -> tuple[atmee.AvatarSession, FakeRoom, FakeAgentSession, dict[str, list[Any]]]:
    fake.script("POST", START, 202, STARTED)
    room, agent_session = FakeRoom(), FakeAgentSession()
    avatar = _avatar(http_session=fake.session, **kwargs)
    events: dict[str, list[Any]] = defaultdict(list)
    for name in ("avatar_disconnected", "avatar_replaced", "avatar_lost"):
        avatar.on(name, lambda ev, name=name: events[name].append(ev))
    await avatar.start(agent_session, room)  # type: ignore[arg-type]
    room.emit("participant_connected", _renderer("PA_1"))
    return avatar, room, agent_session, events


async def _until(condition: Any) -> None:
    for _ in range(200):
        if condition():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition never became true")


def _paths(fake: FakeAtmee) -> list[tuple[str, str]]:
    return [(r.method, r.path) for r in fake.requests if r.method == "POST"]


async def test_a_renderer_that_left_is_replaced(fake: FakeAtmee) -> None:
    avatar, room, agent_session, events = await _started_avatar(fake)
    first = avatar._audio_output
    fake.script("POST", END, 200, {})
    fake.script("POST", START, 202, STARTED2)
    room.emit("participant_disconnected", _renderer("PA_1"))
    await _until(lambda: events["avatar_replaced"])

    # the dead render is ended first, then a new one is asked for, in the room
    # before the agent's voice moves to it, within what is left of the ceiling
    assert _paths(fake) == [("POST", START), ("POST", END), ("POST", START)]
    again = fake.calls("POST", START)[1]
    assert again.query == {"waitFor": "avatar_joined"}
    assert 3590 <= again.json["maxDurationSeconds"] <= 3600
    claims = lk_api.TokenVerifier("APIdevkey", LIVEKIT_SECRET).verify(again.json["livekitToken"])
    assert claims.identity == "atmee-avatar-agent" and claims.video.can_subscribe is False
    assert (
        avatar.session_id == SESSION_ID2 and events["avatar_replaced"][0].session_id == SESSION_ID2
    )
    assert (
        first is not None and first._lost and avatar._audio_output is agent_session.audio_tails[-1]
    )
    assert len(events["avatar_disconnected"]) == 1 and not events["avatar_lost"]

    # the replacement joins; its predecessor being reported gone again changes nothing
    room.emit("participant_connected", _renderer("PA_2"))
    room.emit("participant_disconnected", _renderer("PA_1"))
    await asyncio.sleep(0.02)
    assert len(fake.calls("POST", START)) == 2

    fake.script("POST", END2, 200, {})
    await avatar.aclose()
    assert len(fake.calls("POST", END2)) == 1


async def test_a_renderer_whose_heartbeat_stops_is_replaced(
    fake: FakeAtmee, monkeypatch: pytest.MonkeyPatch
) -> None:
    # hard-killed: it stays in the room, silent, for tens of seconds
    monkeypatch.setattr(atmee_avatar, "_HEARTBEAT_TIMEOUT", 0.05)
    avatar, room, _, events = await _started_avatar(fake)
    fake.script("POST", END, 200, {})
    fake.script("POST", START, 202, STARTED2)
    await asyncio.sleep(0.1)
    assert not events["avatar_replaced"]  # never heard from: never taken for dead
    room.emit("data_received", _status("avatar_worker_heartbeat"))
    await _until(lambda: events["avatar_replaced"])
    assert _paths(fake)[1:] == [("POST", END), ("POST", START)]
    assert not events["avatar_disconnected"]
    fake.script("POST", END2, 200, {})
    await avatar.aclose()


async def test_a_retiring_renderer_plays_on_until_its_replacement_is_in(
    fake: FakeAtmee,
) -> None:
    avatar, room, _, events = await _started_avatar(fake)
    first = avatar._audio_output
    fake.script("POST", START, 202, STARTED2, delay=0.05)
    fake.script("POST", END, 200, {})
    room.emit("data_received", _status("avatar_worker_going_away"))
    await _until(lambda: fake.calls("POST", START)[1:])
    assert first is not None and not first._lost  # still the agent's voice
    await _until(lambda: events["avatar_replaced"])
    # its render ends only once the replacement took over
    assert _paths(fake)[1:] == [("POST", START), ("POST", END)] and first._lost
    # the evicted renderer leaving is not a loss
    room.emit("participant_disconnected", _renderer("PA_1"))
    await asyncio.sleep(0.02)
    assert len(fake.calls("POST", START)) == 2 and not events["avatar_lost"]
    fake.script("POST", END2, 200, {})
    await avatar.aclose()


async def test_without_a_replacement_the_agent_goes_on_without_the_avatar(
    fake: FakeAtmee, monkeypatch: pytest.MonkeyPatch
) -> None:
    # the replacement fails
    avatar, room, _, events = await _started_avatar(fake)
    fake.script("POST", END, 200, {})
    fake.script("POST", START, 503, {"error": "no_capacity"})
    room.emit("participant_disconnected", _renderer("PA_1"))
    await _until(lambda: events["avatar_lost"])
    assert isinstance(events["avatar_lost"][0], AtmeeNoCapacityError)
    assert avatar._audio_output is not None and avatar._audio_output._lost
    await avatar.aclose()

    # replacements are capped, and can be turned off
    monkeypatch.setattr(atmee_avatar, "_MAX_REPLACEMENTS", 0)
    for kwargs in ({}, {"replace_lost_avatar": False}):
        avatar, room, _, events = await _started_avatar(fake, **kwargs)
        fake.script("POST", END, 200, {})
        room.emit("participant_disconnected", _renderer("PA_1"))
        assert events["avatar_lost"] == [None]
        await avatar.aclose()
    assert len(fake.calls("POST", START)) == 4  # 3 starts and the failed replacement


async def test_close_during_a_replacement_ends_the_late_render(fake: FakeAtmee) -> None:
    avatar, room, _, events = await _started_avatar(fake)
    fake.script("POST", END, 200, {})
    fake.script("POST", START, 202, STARTED2, delay=0.2)
    fake.script("POST", END2, 200, {})
    room.emit("participant_disconnected", _renderer("PA_1"))
    await _until(lambda: fake.calls("POST", START)[1:])
    await asyncio.wait_for(avatar.aclose(), timeout=0.15)  # never waits for the request
    await _until(lambda: fake.calls("POST", END2))  # the render it opened is ended
    await _until(lambda: avatar._api._session is None)
    assert not events["avatar_replaced"]
