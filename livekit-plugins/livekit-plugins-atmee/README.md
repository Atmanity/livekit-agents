# Atmee plugin for LiveKit Agents

Give a LiveKit voice agent a face with an [Atmee](https://www.atmee.ai/) avatar. The avatar joins
the room as its own participant and speaks the agent's audio with lips in sync. Your agent keeps its
own speech recognition, model and voice.

Any Atmee avatar renders by its id, v1 (a talking head generated from a single portrait) or v2
(moves as a whole). Create avatars in the [Atmee studio](https://www.atmee.ai/docs/studio/create-an-avatar)
or with the [Atmee API](https://www.atmee.ai/docs/api/create-avatar); this plugin renders them.

See the [Atmee integration docs](https://docs.livekit.io/agents/models/avatar/plugins/atmee/) for more information.

## Installation

```bash
pip install livekit-plugins-atmee
```

## Pre-requisites

You'll need an API key from Atmee, created at [atmee.ai/studio/api-keys](https://www.atmee.ai/studio/api-keys).
It can be set as an environment variable: `ATMEE_API_KEY`

The plugin mints the avatar participant's join token with your own LiveKit credentials
(`LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET`); only that token is sent to Atmee.

## Usage

```python
from livekit.plugins import atmee

avatar = atmee.AvatarSession(avatar_id="...")  # ATMEE_API_KEY from env
await avatar.start(session, room=ctx.room)  # before session.start
await session.start(agent=Agent(...), room=ctx.room)
```

`start()` returns once Atmee's rendering worker has acknowledged the session; the avatar's video
and audio tracks appear in the room a few seconds later. Inside a job, `start()` may run before
`ctx.connect()`, so a slow v2 start overlaps connecting (with the default `wait_for`: the avatar
joins only once your agent is in the room). It first reads the avatar's generation:
a v1 start may take up to 1 minute (3 with `wait_for="avatar_joined"`). The first v2 session after
a quiet period can take a few minutes, so a v2 start may take up to 5 (7.5). The session ends when
your agent leaves the room, when `aclose()` runs (registered as a job shutdown callback), or at
`max_duration_seconds`. If `aclose()` runs while `start()` is still waiting, the render that start
opens is ended and `start()` raises `atmee.AtmeeException` with `code="closed"`.

The avatar's join token grants it only what it needs: it publishes video and audio and sends data,
and subscribes to no one in your room.

### If the avatar leaves

If the avatar participant leaves while your agent is running (its rendering worker died, for
example), the session emits `avatar_disconnected` and the render stops billing. The agent never
waits on the lost avatar: what it was saying ends as interrupted. If the avatar took over a voice
the agent already had (see below), the agent speaks with that voice again; started before the agent
session, the avatar replaced nothing, and the agent goes on without audio. Handle the event to end
the call or start a new `AvatarSession`:

```python
avatar.on("avatar_disconnected", lambda _participant: session.shutdown())
```

### Start the agent first

Started before `session.start()`, the avatar carries the agent's voice from the first word, but a
slow v2 start keeps the user waiting with no agent. To let the agent talk right away and give it a
face once the avatar is in the room, start the avatar after the session, without awaiting it:

```python
await session.start(agent=Agent(...), room=ctx.room)  # the agent speaks with its own voice

async def attach_avatar() -> None:
    avatar = atmee.AvatarSession(avatar_id=avatar_id, wait_for="avatar_joined")
    try:
        await avatar.start(session, room=ctx.room)  # the agent's voice moves to the avatar
    except atmee.AtmeeException:
        logger.warning("continuing without an avatar", exc_info=True)

attach_task = asyncio.create_task(attach_avatar())  # keep a reference
```

## Parameters

| Parameter | Default | Description |
| --- | --- | --- |
| `avatar_id` | required | The Atmee avatar to render. |
| `api_key` | `ATMEE_API_KEY` | Your Atmee API key. |
| `api_url` | `ATMEE_API_URL` or `https://api.atmanity.us` | Atmee API base URL. |
| `avatar_participant_identity` | `"atmee-avatar-agent"` | The LiveKit identity the avatar joins under. Must be unique per concurrent avatar in a room. |
| `avatar_participant_name` | `"atmee-avatar-agent"` | The LiveKit display name the avatar joins under. |
| `max_duration_seconds` | `3600` | Hard ceiling of the session. |
| `wait_for` | `"initializing"` | `"avatar_joined"` makes `start()` block until the avatar is in the room. |
| `metadata` | `None` | Free-form JSON stored with the session on the Atmee side. |

Every failure raises `atmee.AtmeeException`, a `livekit.agents.APIError`, with `status_code`,
`code`, `message` and `retryable`. An error answer from the API is an `atmee.AtmeeStatusError`
(also an `APIStatusError`, with the response in `body`); no answer is an
`atmee.AtmeeConnectionError` (also an `APIConnectionError`). Subclasses worth handling:

- `atmee.AtmeeNoCapacityError`: every rendering worker is busy; try again after `retry_after`.
- `atmee.AtmeeAvatarWarmingError`: a v2 avatar was still getting ready when the start's wait ran
  out; nothing was billed, and you can try again after `retry_after`.
- `atmee.AtmeeAvatarNotReadyError`: the avatar has nothing to render yet.
- `atmee.AtmeeAvatarBusyError`: the avatar is in another live session of your account
  (`live_session_id`); end it, or try again once it has ended.
