import 'dart:io';

import '../http.dart';
import '../protocol.dart';
import '../types.dart';

/// What a transport needs from the connection.
abstract class TransportHost {
  SignalRHttp get http;
  HttpClient get httpClient;
  Map<String, String> get requestHeaders;

  /// `/connect` (or `/reconnect` with messageId and groupsToken).
  Uri connectUri(TransportType type, {required bool reconnect});

  /// Long polling: `/poll` (or `/reconnect`).
  Uri pollUri({required bool reconnect});

  Uri sendUri(TransportType type);

  Duration get transportConnectTimeout;
  Duration get pollTimeout;
  Duration get longPollDelay;
  Duration get reconnectDelay;

  void log(LogLevel level, String Function() message);

  /// Long polling marks the connection active whenever a poll is issued.
  void markActive();

  /// Hands a raw payload to the connection.
  ResponseInfo onTransportData(ClientTransport transport, String data);

  /// The transport closed or failed after it had started. Called at most once.
  void onTransportLost(ClientTransport transport, Object error);
}

abstract class ClientTransport {
  TransportType get type;

  /// Whether the server sends keep-alives over this transport.
  bool get supportsKeepAlive;

  /// Opens the transport. For a first connect, completes once it is open
  /// (the connection then waits for the init message). For a reconnect,
  /// completes once the transport is considered reconnected.
  Future<void> start({required bool reconnect});

  Future<void> send(String payload);

  /// Closes the transport. Idempotent; no callbacks after this.
  Future<void> close();
}

/// `POST /send` used by Server-Sent Events and Long Polling. The response may
/// carry the invocation result, so it is processed like any other payload.
Future<void> sendOverHttp(TransportHost host, ClientTransport transport, String payload) async {
  final body = await host.http.post(
    host.sendUri(transport.type),
    'data=${Uri.encodeQueryComponent(payload)}',
  );
  host.onTransportData(transport, body);
}
