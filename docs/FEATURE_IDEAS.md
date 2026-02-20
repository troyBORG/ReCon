# ReCon Feature Ideas

Ideas for new features in ReCon, informed by the **FrooxEngine** (Resonite engine) and **ResoniteLink** (WebSocket protocol + client) ecosystems. ReCon is the Resonite companion app (contacts, chat, sessions, worlds, inventory).

---

## 0. Refined ideas (from feedback)

These sharpen or extend ideas from the sections below.

### 0.1 ResoniteLink “mini inspector” in the app

- **Idea:** A **mini world inspector** inside ReCon: connect to your session via ResoniteLink (when you’re host) and show the **root of the world** — e.g. `GetSlot("Root")` and one level of children (or a small collapsible tree). Read-only; no need to put the headset on to see “what’s there.”
- **Value:** Quick glance at world structure from the phone; feels like a dev-tools inspector for your session.
- **Notes:** Same account = you’re host; user configures Link port (or app discovers it if the API ever exposes it). Start with Root + direct children; expand later.

### 0.2 “My current session” in the Sessions tab (same account)

- **Idea:** When you’re playing Resonite (same account as ReCon), show **your current session** prominently in the Sessions tab. Use the API to find sessions where you’re the host or in `sessionUsers`, then show that session with:
  - Everyone in the session (already in `Session.sessionUsers`),
  - World/session info from the API,
  - Optional: resolve or show **go.resonite.com** join link for that world so you can share it or open it.
- **Value:** One place to see “where I am” and who’s with you; easy to copy join link for friends.
- **Notes:** Requires API support for “sessions I’m in” (e.g. filter by current user/machine) if not already available.

### 0.3 “User is now online” popup (like in-game)

- **Idea:** When a friend comes online, show a **popup or snackbar**: “*Username* is now online” — similar to in-game presence. Use Hub’s **receiveStatusUpdate**; optional setting to enable/disable or limit to certain friends.
- **Value:** Same “friend came online” awareness as in Resonite, but in the app.

### 0.4 Voice messages: send as well as listen

- **Idea:** ReCon already supports **sending** voice (hold mic in input bar → record → upload → send). Make it **obvious and reliable**: e.g. a clear mic button, visible “recording…” state, and ensure recording/upload works on all supported devices. If anything is broken (permissions, encoding, or UI), fix it so “send voice” is a first-class option next to text and images.
- **Value:** Full two-way voice in chat from the app, not just playback.

---

## 1. ResoniteLink / In-World Integration

### 1.1 Optional “Link to my session” (ResoniteLink client in ReCon)

- **Idea:** When the user is hosting a Resonite session, allow ReCon to connect via **ResoniteLink** (WebSocket to the host’s Link port) for read-only or limited write access.
- **Value:** From the phone/companion app: view session/world structure, see who’s in-world, maybe trigger simple in-world actions (e.g. “ping” a slot, change a sign).
- **Notes:** Requires user to be host and to expose ResoniteLink (port config). ResoniteLink is request/response only today; no push from Resonite. Could start with “session alive + basic GetSessionData” and expand.

### 1.2 “Session dashboard” / mini inspector from Link

- **Idea:** Use ResoniteLink’s **GetSlot("Root")** and children (or **GetComponent** for key types) to show a **mini inspector** in ReCon: root slot plus a simple tree of direct children (names, maybe component counts). Expandable/collapsible if desired.
- **Value:** Glance at “what’s in the world” without putting the headset on; like a tiny scene hierarchy view.
- **Notes:** Same as above: host must have Link enabled; polling is acceptable for a dashboard.

### 1.3 Watch for world changes (poll-based, ResoniteLink-style)

- **Idea:** Reuse the **WorldWatcher** pattern from ResoniteLink (periodic GetSlot snapshots, diff slot/component counts and types) and surface “world changed” or “slots added/removed” in ReCon.
- **Value:** Notifications like “Someone added 5 slots to your world” or “Component X appeared,” useful for builders.
- **Notes:** ResoniteLink has no push; polling interval and battery/CPU tradeoffs matter on mobile.

---

## 2. Sessions & Worlds (Beyond Current API)

### 2.1 Richer session actions from the app

- **Idea:** Use existing Session API plus any future/undocumented endpoints to add: “Request invite,” “Copy session link,” “Notify friends this session is open,” or “Set session to private/public” from ReCon.
- **Value:** Manage sessions from the phone without opening the game.
- **Notes:** Depends on what `api.resonite.com` exposes; some may need backend support.

### 2.2 World favorites / recent worlds

- **Idea:** Persist “favorite” or “recent” worlds in ReCon (e.g. Hive) and show a quick-access list in addition to search.
- **Value:** Faster access to frequently used worlds.

### 2.3 World/session thumbnails in lists

- **Idea:** If the API provides session/world thumbnails (e.g. **SessionThumbnailData**-style URLs), show them in session and world list UIs.
- **Value:** Easier recognition of sessions/worlds at a glance.

---

## 3. Inventory & Records (Record API + FrooxEngine “records”)

### 3.1 Inventory search and filters

- **Idea:** Full-text or name/type search over the current directory (and optionally children), plus filters (e.g. “only images,” “only folders,” “modified in last week”).
- **Value:** Large inventories become manageable from the app.
- **Note:** ReCon only searches the current folder (no full-inventory crawl) to keep the app light.

### 3.2 Bulk operations in inventory

- **Idea:** Multi-select then: “Move all to folder,” “Download selected,” “Delete selected,” “Copy record links to clipboard.”
- **Value:** Faster inventory management; aligns with existing copy/move in ReCon.

### 3.3 “Save to inventory” from more surfaces

- **Idea:** Extend “save to inventory” (today on message assets) to sessions/worlds: e.g. “Save this world’s record to my inventory” or “Save session info as a record.”
- **Value:** One place for “things I want to keep” from chat, sessions, and worlds.

### 3.4 Record URL handling (FrooxEngine record URLs)

- **Idea:** FrooxEngine uses **record URLs** in **SavedGraph** and **RecursiveRecordProcessor**. ReCon could: (a) show “record URL” for inventory items (for use in-game or in tools), (b) parse record URLs from messages or links and open the record in ReCon.
- **Value:** Better interoperability with in-world content and external tools.

### 3.5 Preprocess / publish status in UI

- **Idea:** Surface **preprocess** and publish state (e.g. “Processing…”, “Public”, “Patron”) for records in the inventory UI, with retry or “preprocess now” where the API allows.
- **Value:** Clear feedback when uploading or publishing from the app.

---

## 4. Chat & Messaging

### 4.1 Notification actions (Android)

- **Idea:** Implement the existing TODO: “Clicking message notification opens chat of specified user.”
- **Value:** Direct jump to conversation from a notification.

### 4.2 Message search

- **Idea:** Search messages by text (and optionally by sender/date) within a conversation or globally.
- **Value:** Find old links, decisions, or content without scrolling.

### 4.3 Quick replies / templates

- **Idea:** User-defined short replies or templates (e.g. “On my way,” “In VR, back in 30”) for fast responses from the phone.
- **Value:** Faster communication when not typing.

### 4.4 Unread / mentions

- **Idea:** Clear unread state and, if the API supports it, highlight or filter “mentions” of the current user.
- **Value:** Better awareness of what needs a response.

---

## 5. Notifications & Presence

### 5.1 “Friend joined a session” / session presence

- **Idea:** Use Hub events (e.g. **receiveSessionUpdate**) and session list to show “Friend X is in session Y” and optionally “Join” deep link or notification.
- **Value:** Easier to join friends without polling the session list manually.

### 5.2 “User is now online” popup (like in-game)

- **Idea:** On **receiveStatusUpdate**, when a friend transitions to online (or sociable/away/busy), show a **popup or snackbar**: “*Username* is now online.” Optional setting to turn on/off or limit to certain friends.
- **Value:** Same in-app awareness of friends coming online as in Resonite.

### 5.3 Configurable notification rules

- **Idea:** Settings for: “Notify for all messages” vs “Only when mentioned” vs “Only for certain friends,” and “Notify when friends come online.”
- **Value:** Less noise, more signal on mobile.

### 5.4 Desktop parity for notifications

- **Idea:** Where possible (e.g. Linux/Windows with appropriate plugins), support the same notification and “open chat” behavior as on Android.
- **Value:** Consistent experience across platforms.

---

## 6. Media & Assets

### 6.1 Voice: send + listen, and list UX

- **Idea:** (1) **Sending:** Keep send-voice discoverable and reliable (dedicated mic button, clear “recording…” state, fix any permission/encoding issues). (2) **List:** Show duration and optionally a simple waveform or “voice” icon in the message list for voice clips.
- **Value:** Full two-way voice in chat; quick recognition of voice vs text in the list.

### 6.2 In-app image editing before send

- **Idea:** Simple crop, rotate, or draw before uploading an image (e.g. for screenshots or quick markup).
- **Value:** Fewer round-trips to another app.

### 6.3 Support more asset types in messages

- **Idea:** Align with Record API and Resonite asset types: e.g. “upload as 3D model” or “upload as audio” with correct type/labels so in-game or other clients can use them properly.
- **Value:** Richer message content and inventory from the app.

---

## 7. Reliability & UX

### 7.1 Hub reconnect and session recovery

- **Idea:** On Hub disconnect, show “Reconnecting…” and, after reconnect, refresh session/message state and avoid duplicate sends or missed updates.
- **Value:** More reliable chat and presence on flaky networks.

### 7.2 Logout cleanup (existing TODO)

- **Idea:** “Fix messaging/hub clients not being disposed on logout” so state and connections are fully cleared.
- **Value:** Clean logout and no leaks between accounts.

### 7.3 Offline / cached view

- **Idea:** Cache recent conversations and contacts (e.g. in Hive) so the app shows last-known state when offline, with clear “offline” indicator.
- **Value:** Usable in poor connectivity; can read recent chats offline.

---

## 8. “Stretch” / Future (when APIs or Link evolve)

### 8.1 ResoniteLink push or “world changed” events

- **Idea:** If Resonite/ResoniteLink later add “push” or “subscribe to field/slot” events, ReCon could show live world/session updates (e.g. “Slot X changed”) without polling.
- **Value:** Real-time awareness of in-world changes from the companion app.

### 8.2 MCP or automation bridge

- **Idea:** ResoniteLink has an **MCP server**; ReCon could expose a small “automation” surface (e.g. “Send this message when X” or “Notify when slot count &gt; N”) that talks to a local MCP client or script.
- **Value:** Power users could integrate ReCon with other tools (e.g. Discord, streaming).

### 8.3 Screenshot or camera API (ResoniteLink roadmap)

- **Idea:** If ResoniteLink adds a screenshot/camera API, ReCon could request a “current view” from the running session and show it in the app.
- **Value:** “What I see in VR” on the phone without taking a manual screenshot.

---

## Summary table

| Category              | Idea (short)                          | Depends on              |
|-----------------------|----------------------------------------|-------------------------|
| **Refined**           | Mini inspector (root + children)      | User is host, Link     |
| **Refined**           | “My current session” in Sessions tab   | API (sessions I’m in)   |
| **Refined**           | “User is now online” popup            | ReCon + Hub             |
| **Refined**           | Voice: send obvious + reliable        | ReCon                   |
| ResoniteLink          | Link to my session                     | User is host, Link port |
| ResoniteLink          | Session dashboard / mini inspector     | Same                    |
| ResoniteLink          | Watch world changes (poll)             | Same                    |
| Sessions/Worlds       | Richer session actions                 | API support             |
| Sessions/Worlds       | World favorites / recent               | ReCon only              |
| Sessions/Worlds       | Thumbnails in lists                    | API                     |
| Sessions/Worlds       | go.resonite.com join link for session  | Session API / URL       |
| Inventory             | Search and filters                     | ReCon + Record API      |
| Inventory             | Bulk operations                        | ReCon                   |
| Inventory             | Save to inventory from more places     | ReCon                   |
| Inventory             | Record URL handling                    | ReCon + docs            |
| Inventory             | Preprocess/publish status in UI        | Record API              |
| Chat                  | Notification → open chat               | ReCon (TODO)            |
| Chat                  | Message search                         | Message API             |
| Chat                  | Quick replies / templates             | ReCon                   |
| Notifications         | Friend joined session                 | Hub + Session API       |
| Notifications         | “User is now online” popup             | ReCon + Hub             |
| Notifications         | Configurable notification rules        | ReCon                   |
| Notifications         | Desktop notification parity            | Platform                |
| Media                 | Voice: send + list (duration/waveform) | ReCon                   |
| Media                 | In-app image edit before send          | ReCon                   |
| Media                 | More asset types in messages           | Record API              |
| Reliability           | Hub reconnect / session recovery       | ReCon                   |
| Reliability           | Logout cleanup                         | ReCon (TODO)            |
| Reliability           | Offline / cached view                  | ReCon                   |
| Stretch               | Link push / world events               | ResoniteLink future     |
| Stretch               | MCP / automation bridge                | ReCon + MCP             |
| Stretch               | Screenshot from session                | ResoniteLink roadmap    |

---

*Generated from exploration of ReCon, FrooxEngineDecompile, and ResoniteLink. Prioritization and implementation order are left to the maintainers.*
