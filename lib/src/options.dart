import 'dart:io';

import 'types.dart';

class HubConnectionOptions {
  const HubConnectionOptions({
    this.queryString,
    this.queryParameters,
    this.headers,
    this.transport = TransportType.auto,
    this.useDefaultPath = true,
    this.clientProtocol = '1.5',
    this.transportConnectTimeout,
    this.httpTimeout = const Duration(seconds: 30),
    this.reconnectDelay = const Duration(seconds: 2),
    this.reconnectWindow,
    this.invocationTimeout,
    this.httpClientFactory,
    this.logger,
    this.logMessageContent = false,
  });

  /// Raw query string appended to every request, exactly as given
  /// (e.g. `session=abc`). This is what the signalr_flutter plugin did.
  final String? queryString;

  /// Query parameters appended to every request (URL-encoded).
  final Map<String, String>? queryParameters;

  /// Headers sent with every HTTP and WebSocket request.
  final Map<String, String>? headers;

  final TransportType transport;

  /// Append `/signalr` to the URL, like `$.hubConnection(url)` and the Java
  /// client. Set to false when the URL already points at the SignalR endpoint.
  final bool useDefaultPath;

  /// Protocol version requested. If the server answers with another supported
  /// version (1.2 - 1.5), the client adapts to it.
  final String clientProtocol;

  /// Time allowed for a transport to open (and receive the init message).
  /// Defaults to the server's TransportConnectTimeout, at least 5 seconds.
  final Duration? transportConnectTimeout;

  /// Timeout for negotiate, start, send and abort requests.
  final Duration httpTimeout;

  /// Delay between reconnect attempts (official clients: 2 seconds).
  final Duration reconnectDelay;

  /// How long to keep trying to reconnect. Defaults to the server's
  /// KeepAliveTimeout + DisconnectTimeout (official JavaScript client).
  final Duration? reconnectWindow;

  /// Default timeout for hub invocations. Null waits until the connection
  /// drops or stops.
  final Duration? invocationTimeout;

  /// Supplies the HttpClient used for every request (for example one with
  /// certificate pinning). When null, `HttpClient()` is used, which honours
  /// `HttpOverrides.global`.
  final HttpClient Function()? httpClientFactory;

  final SignalRLogger? logger;

  /// Include message payloads in log output. Keep false in production:
  /// payloads may contain personal data.
  final bool logMessageContent;
}
