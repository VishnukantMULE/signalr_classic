import 'dart:async';
import 'dart:convert';
import 'dart:io';

import '../exceptions.dart';
import '../types.dart';
import 'transport.dart';

class WebSocketTransport implements ClientTransport {
  WebSocketTransport(this._host);

  final TransportHost _host;
  WebSocket? _socket;
  StreamSubscription<dynamic>? _subscription;
  bool _closed = false;

  @override
  TransportType get type => TransportType.webSockets;

  @override
  bool get supportsKeepAlive => true;

  @override
  Future<void> start({required bool reconnect}) async {
    final uri = _host.connectUri(type, reconnect: reconnect);
    final wsUri = uri.replace(scheme: uri.scheme == 'https' ? 'wss' : 'ws');
    final connecting = WebSocket.connect(
      wsUri.toString(),
      headers: _host.requestHeaders,
      customClient: _host.httpClient,
    );
    WebSocket socket;
    try {
      socket = await connecting.timeout(_host.transportConnectTimeout);
    } on TimeoutException {
      // Close the socket if it opens after we gave up on it.
      unawaited(connecting.then((s) => s.close()).catchError((Object _) {}));
      throw TransportException('WebSocket did not open within '
          '${_host.transportConnectTimeout.inMilliseconds} ms');
    } on WebSocketException catch (e) {
      throw TransportException('WebSocket connection failed', e);
    }
    if (_closed) {
      await socket.close();
      throw TransportException('Transport closed while opening');
    }
    _socket = socket;
    _subscription = socket.listen(
      (dynamic data) {
        if (_closed) return;
        final text = data is String ? data : utf8.decode(data as List<int>, allowMalformed: true);
        _host.onTransportData(this, text);
      },
      onError: (Object e) => _lost(TransportException('WebSocket error', e)),
      onDone: () => _lost(TransportException(
          'WebSocket closed (code ${socket.closeCode}${socket.closeReason == null || socket.closeReason!.isEmpty ? '' : ', ${socket.closeReason}'})')),
      cancelOnError: true,
    );
  }

  void _lost(Object error) {
    if (_closed) return;
    _closed = true;
    _host.onTransportLost(this, error);
  }

  @override
  Future<void> send(String payload) async {
    final socket = _socket;
    if (socket == null || _closed) throw TransportException('WebSocket is not open');
    socket.add(payload);
  }

  @override
  Future<void> close() async {
    _closed = true;
    final socket = _socket;
    _socket = null;
    await _subscription?.cancel();
    _subscription = null;
    if (socket != null) {
      try {
        await socket.close(WebSocketStatus.normalClosure).timeout(const Duration(seconds: 2));
      } catch (_) {
        // Already closed or unreachable; nothing else to do.
      }
    }
  }
}
