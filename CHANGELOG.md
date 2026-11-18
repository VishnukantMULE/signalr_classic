# Changelog

## 1.0.0

- First version: pure-Dart client for classic ASP.NET SignalR 2.x.
- Transports: WebSockets, Server-Sent Events, Long Polling; `auto` falls back in that order.
- Protocol versions 1.2 to 1.5, adapting to the version the server answers with.
- Keep-alive monitoring (`connectionSlow` at 2/3 of KeepAliveTimeout), automatic reconnect with
  messageId and groupsToken, reconnect window, server `T`/`D`/`G`/`C`/`L` commands.
- Hub proxies: several handlers per method, hub state, progress updates, invocation timeouts.
- `signalr_flutter_compat.dart`: drop-in replacement for the `signalr_flutter` plugin API.
