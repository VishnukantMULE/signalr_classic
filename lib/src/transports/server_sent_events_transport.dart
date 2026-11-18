import 'dart:async';
import 'dart:convert';
import 'dart:io';

import '../exceptions.dart';
import '../types.dart';
import 'transport.dart';

class ServerSentEventsTransport implements ClientTransport {
  ServerSentEventsTransport(this._host);

  final TransportHost _host;
  HttpClientRequest? _request;
  StreamSubscription<String>? _subscription;
  Completer<void>? _opened;
  final List<String> _dataLines = <String>[];
  bool _closed = false;

  @override
  TransportType get type => TransportType.serverSentEvents;

  @override
  bool get supportsKeepAlive => true;

  @override
  Future<void> start({required bool reconnect}) async {
    final opened = Completer<void>();
    _opened = opened;
    final response = await _host.http.openStream(
      _host.connectUri(type, reconnect: reconnect),
      extraHeaders: const <String, String>{
        'Accept': 'text/event-stream',
        'Cache-Control': 'no-cache',
      },
      timeout: _host.transportConnectTimeout,
      onRequest: (r) => _request = r,
    );
    if (_closed) {
      unawaited(response.drain<void>().catchError((Object _) {}));
      throw TransportException('Transport closed while opening');
    }
    _subscription = response.transform(utf8.decoder).transform(const LineSplitter()).listen(
          _onLine,
          onError: (Object e) => _fail(TransportException('Server-Sent Events stream error', e)),
          onDone: () => _fail(TransportException('Server-Sent Events stream ended')),
          cancelOnError: true,
        );
    try {
      await opened.future.timeout(_host.transportConnectTimeout);
    } on TimeoutException {
      await close();
      throw TransportException('No "initialized" event within '
          '${_host.transportConnectTimeout.inMilliseconds} ms');
    }
  }

  /// Server-Sent Events framing: `data:` lines accumulate until a blank line.
  void _onLine(String line) {
    if (_closed) return;
    if (line.isEmpty) {
      if (_dataLines.isEmpty) return;
      final data = _dataLines.join('\n');
      _dataLines.clear();
      _dispatch(data);
      return;
    }
    if (line.startsWith(':')) return; // comment
    final colon = line.indexOf(':');
    final field = colon < 0 ? line : line.substring(0, colon);
    var value = colon < 0 ? '' : line.substring(colon + 1);
    if (value.startsWith(' ')) value = value.substring(1);
    if (field == 'data') _dataLines.add(value);
  }

  void _dispatch(String data) {
    final opened = _opened;
    if (data == 'initialized') {
      if (opened != null && !opened.isCompleted) opened.complete();
      return;
    }
    // A server that skips "initialized" is open once it sends anything.
    if (opened != null && !opened.isCompleted) opened.complete();
    _host.onTransportData(this, data);
  }

  void _fail(Object error) {
    if (_closed) return;
    final opened = _opened;
    if (opened != null && !opened.isCompleted) {
      _closed = true;
      opened.completeError(error);
      return;
    }
    _closed = true;
    _host.onTransportLost(this, error);
  }

  @override
  Future<void> send(String payload) {
    if (_closed) return Future<void>.error(TransportException('Server-Sent Events transport is closed'));
    return sendOverHttp(_host, this, payload);
  }

  @override
  Future<void> close() async {
    _closed = true;
    _request?.abort();
    _request = null;
    await _subscription?.cancel();
    _subscription = null;
  }
}
