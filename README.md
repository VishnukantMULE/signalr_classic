# signalr_classic

A pure-Dart client for **classic ASP.NET SignalR 2.x** hubs (the .NET Framework
version, not ASP.NET Core). It runs the same code on Android and iOS: no native
plugin, no Java client and no hidden WebView.

It also includes a **drop-in replacement for the `signalr_flutter` plugin API**,
so an app can switch by changing one import.

## Features

| Area | Support |
|---|---|
| Transports | WebSockets, Server-Sent Events, Long Polling. `auto` tries them in that order (WebSockets only when the server allows it). |
| Protocol | Versions 1.2 to 1.5 (SignalR 1.x to 2.4). Requests 1.5 and adapts if the server answers with an older supported version; `/start` is used from 1.4. |
| Keep-alive | `connectionSlow` at 2/3 of the server's KeepAliveTimeout; the transport counts as lost at KeepAliveTimeout. |
| Reconnect | Automatic, every `reconnectDelay` (2 s), resuming from `messageId` and `groupsToken`, until KeepAliveTimeout + DisconnectTimeout has passed. 401/403 stops at once. After a long app suspension it closes instead of trying a reconnect that cannot succeed. |
| Server commands | `T` (reconnect), `D` (disconnect), `G` (groups token), `C` (cursor), `L` (long-poll delay), `S` (init). |
| Hubs | Several hubs per connection, several handlers per method (case-insensitive), hub state, progress updates, invocation results and errors (`HubException` with `H`/`D`/`T`). |
| Safety | Pending invocations always complete (result, error, timeout or `ConnectionClosedException`); a throwing handler never breaks the connection; invalid JSON is reported, not fatal. |
| Configuration | Query string or parameters, headers on every request (including the WebSocket upgrade), custom `HttpClient` (e.g. certificate pinning; `HttpOverrides.global` is honoured by default), timeouts, logger. |
| Privacy | Logs never contain connection tokens, headers or query strings; payloads are logged only with `logMessageContent: true`. |
| Platforms | Android, iOS, macOS, Windows, Linux (`dart:io`). **Not web.** |
| Dart | SDK `>=2.18.0 <4.0.0`. No records, patterns or sealed classes, so it can be used from Dart 2.18 language level. |

## Usage

```dart
import 'package:signalr_classic/signalr_classic.dart';

final connection = HubConnection(
  'https://example.com/myapp',            // `/signalr` is appended
  options: HubConnectionOptions(queryString: 'session=$session'),
);
final hub = connection.createHubProxy('markethub'); // before start()
hub.on('PriceUpdate', (args) => handle(args.first));

connection.onStateChanged.listen((c) => print('${c.previous} -> ${c.current}'));
connection.onConnectionSlow.listen((_) => print('slow'));
connection.onClosed.listen((error) => print('closed: $error'));

await connection.start();                     // throws SignalRException subclasses on failure
final result = await hub.invoke('Handshake', [user]);
await connection.stop();                      // or dispose() when done with it
```

## Migrating from `signalr_flutter`

Change the import and nothing else:

```dart
// import 'package:signalr_flutter/signalr_flutter.dart';
import 'package:signalr_classic/signalr_flutter_compat.dart';
```

`SignalR(baseUrl, hubName, queryString:, headers:, hubMethods:, transport:, statusChangeCallback:, hubCallback:, connectionErrorCallback:)`,
`connect()`, `reconnect()`, `stop()`, `isConnected()`, `invokeMethod()`, `connectionId`,
`ConnectionStatus` and `Transport` keep the same shape and values. So code comparing
`'$status' == 'ConnectionStatus.connected'` keeps working.

Intentional differences from the plugin:

| | `signalr_flutter` | `signalr_classic` compat |
|---|---|---|
| Android | Microsoft Java client; **no WebSockets** (SSE, then Long Polling) | WebSockets, then SSE, then Long Polling |
| iOS | jQuery 2.1.3 + jquery.signalR inside a hidden WKWebView | Pure Dart, no WebView |
| Failed `invokeMethod` | Never completes on Android | Throws |
| Short network drop | `connectionError` (Android) then reconnect | `reconnecting` then `connected`; `connectionError` only when the connection really fails. Set `reportTransientErrors: true` for the old behaviour. |
| `connectionId` | Often `''` (returned before connecting) | Set once connected |

## Testing

```bash
dart pub get
dart analyze
dart test            # 110 tests against an in-process fake classic SignalR server
```

The fake server covers all three transports, protocol versions 1.2 to 1.5,
keep-alive, reconnect (messageId, groupsToken), server commands, failures (401/403/500,
bad JSON, missing init), progress, hub state, 1 MB payloads and concurrency.

### Against a real hub

Run on a network that can reach the server:

```bash
dart run tool/live_check.dart --url https://example.com/myapp --hub markethub --methods PriceUpdate --seconds 60
dart run tool/live_check.dart --url https://example.com/myapp --hub markethub --transport serverSentEvents
dart run tool/live_check.dart --url https://example.com/myapp --hub markethub \
  --query session=<session> --invoke Handshake --args <user> --verbose
```
