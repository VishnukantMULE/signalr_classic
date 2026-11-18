import 'dart:async';
import 'dart:convert';
import 'dart:io';

class RecordedRequest {
  RecordedRequest(this.method, this.uri, this.testHeader, this.body);

  final String method;
  final Uri uri;
  final String? testHeader;
  final String body;

  String get action => uri.pathSegments.isEmpty ? '' : uri.pathSegments.last;
  Map<String, String> get query => uri.queryParameters;
}

/// Reply for a hub invocation.
class Reply {
  Reply(
      {this.result,
      this.state,
      this.error,
      this.isHubError = false,
      this.errorData,
      this.progress = const <dynamic>[]});

  final dynamic result;
  final Map<String, dynamic>? state;
  final String? error;
  final bool isHubError;
  final dynamic errorData;
  final List<dynamic> progress;
}

/// In-process classic ASP.NET SignalR 2.x server for tests.
class FakeServer {
  FakeServer({
    this.protocolVersion,
    this.keepAliveSeconds = 20,
    this.disconnectSeconds = 30,
    this.connectionTimeoutSeconds = 1,
    this.tryWebSockets = true,
    this.transportConnectTimeoutSeconds = 5,
    this.longPollDelaySeconds = 0,
  });

  /// Null: echo the client's clientProtocol.
  String? protocolVersion;
  num? keepAliveSeconds;
  num disconnectSeconds;
  num connectionTimeoutSeconds;
  bool tryWebSockets;
  num transportConnectTimeoutSeconds;
  num longPollDelaySeconds;

  bool allowWebSockets = true;
  bool allowServerSentEvents = true;
  bool allowLongPolling = true;
  bool sendInitMessage = true;
  bool sendSseInitialized = true;
  bool silentInvocations = false;
  bool rejectReconnect = false;
  int rejectReconnectStatus = 404;
  int negotiateStatus = 200;
  String? negotiateBodyOverride;
  Duration negotiateDelay = Duration.zero;
  int startStatus = 200;
  String startBody = '{"Response":"started"}';

  Reply Function(String hub, String method, List<dynamic> args, Map<String, dynamic>? state)? onInvoke;

  final List<RecordedRequest> requests = <RecordedRequest>[];
  final List<Map<String, dynamic>> invocations = <Map<String, dynamic>>[];

  late HttpServer _server;
  WebSocket? _ws;
  HttpResponse? _sse;
  final List<Map<String, dynamic>> _lpQueue = <Map<String, dynamic>>[];
  Completer<void>? _lpWaiter;
  int _failPolls = 0;
  int _cursor = 0;
  String? connectedTransport;

  String get baseUrl => 'http://${_server.address.host}:${_server.port}/app';

  List<RecordedRequest> of(String action) => requests.where((r) => r.action == action).toList();
  RecordedRequest last(String action) => requests.lastWhere((r) => r.action == action);

  Future<void> start() async {
    _server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    _server.listen((req) {
      _handle(req).catchError((Object _) {});
    });
  }

  Future<void> close() async {
    try {
      await _ws?.close();
    } catch (_) {}
    try {
      await _sse?.close();
    } catch (_) {}
    _lpWaiter?.complete();
    await _server.close(force: true);
  }

  // ------------------------------------------------------------- server push

  void push(String hub, String method, List<dynamic> args, {Map<String, dynamic>? state}) {
    sendFrame(<String, dynamic>{
      'C': 'd-${++_cursor}',
      'M': <dynamic>[
        <String, dynamic>{'H': hub, 'M': method, 'A': args, if (state != null) 'S': state},
      ],
    });
  }

  void pushMany(List<Map<String, dynamic>> messages) {
    sendFrame(<String, dynamic>{'C': 'd-${++_cursor}', 'M': messages});
  }

  void keepAlive() => sendFrame(<String, dynamic>{});

  void sendFrame(Map<String, dynamic> frame) {
    final text = jsonEncode(frame);
    if (connectedTransport == 'webSockets') {
      _ws?.add(text);
    } else if (connectedTransport == 'serverSentEvents') {
      _sse?.write('data: $text\n\n');
    } else if (connectedTransport == 'longPolling') {
      _lpQueue.add(frame);
      _wakePoll();
    }
  }

  void sendRawText(String text) {
    if (connectedTransport == 'webSockets') {
      _ws?.add(text);
    } else if (connectedTransport == 'serverSentEvents') {
      _sse?.write('data: $text\n\n');
    }
  }

  /// Drops the transport without telling the client.
  Future<void> dropTransport() async {
    if (connectedTransport == 'longPolling') {
      _failPolls = 1;
      _wakePoll();
      return;
    }
    final ws = _ws;
    final sse = _sse;
    _ws = null;
    _sse = null;
    try {
      await ws?.close();
    } catch (_) {}
    try {
      await sse?.close();
    } catch (_) {}
  }

  void _wakePoll() {
    final w = _lpWaiter;
    _lpWaiter = null;
    if (w != null && !w.isCompleted) w.complete();
  }

  // ---------------------------------------------------------------- handler

  Future<void> _handle(HttpRequest req) async {
    final body = req.method == 'POST' ? await utf8.decoder.bind(req).join() : '';
    final rec = RecordedRequest(req.method, req.uri, req.headers.value('x-test'), body);
    requests.add(rec);
    final transport = req.uri.queryParameters['transport'];
    switch (rec.action) {
      case 'negotiate':
        if (negotiateDelay > Duration.zero) await Future<void>.delayed(negotiateDelay);
        if (negotiateStatus != 200) return _status(req, negotiateStatus);
        return _write(
            req,
            negotiateBodyOverride ??
                jsonEncode(<String, dynamic>{
                  'Url': '/app/signalr',
                  'ConnectionToken': 'tok+/=AQ',
                  'ConnectionId': 'cid-1',
                  'KeepAliveTimeout': keepAliveSeconds,
                  'DisconnectTimeout': disconnectSeconds,
                  'ConnectionTimeout': connectionTimeoutSeconds,
                  'TryWebSockets': tryWebSockets,
                  'ProtocolVersion': protocolVersion ?? req.uri.queryParameters['clientProtocol'],
                  'TransportConnectTimeout': transportConnectTimeoutSeconds,
                  'LongPollDelay': longPollDelaySeconds,
                }));
      case 'connect':
      case 'reconnect':
        final reconnect = rec.action == 'reconnect';
        if (reconnect && rejectReconnect) return _status(req, rejectReconnectStatus);
        if (transport == 'webSockets') return _webSocket(req, reconnect);
        if (transport == 'serverSentEvents') return _serverSentEvents(req, reconnect);
        if (transport == 'longPolling') return _longPoll(req, initial: !reconnect, reconnect: reconnect);
        return _status(req, 400);
      case 'poll':
        return _longPoll(req, initial: false, reconnect: false);
      case 'start':
        if (startStatus != 200) return _status(req, startStatus);
        return _write(req, startBody);
      case 'send':
        final data = Uri.splitQueryString(body)['data'] ?? '';
        final replies = <String>[];
        _invoke(data, replies.add);
        // Progress (if any) goes over the stream; the result is the response.
        final result = replies.isEmpty ? '' : replies.removeLast();
        for (final p in replies) {
          sendRawText(p);
        }
        return _write(req, result);
      case 'abort':
        return _write(req, '');
      default:
        return _status(req, 404);
    }
  }

  Future<void> _webSocket(HttpRequest req, bool reconnect) async {
    if (!allowWebSockets || !WebSocketTransformer.isUpgradeRequest(req)) return _status(req, 400);
    // ignore: close_sinks (kept open on purpose; closed in close()/dropTransport())
    final ws = await WebSocketTransformer.upgrade(req);
    _ws = ws;
    connectedTransport = 'webSockets';
    ws.listen((dynamic data) => _invoke(data as String, ws.add), onError: (Object _) {});
    if (!reconnect && sendInitMessage) {
      ws.add(jsonEncode(<String, dynamic>{'C': 's-0', 'S': 1, 'M': <dynamic>[]}));
    }
  }

  Future<void> _serverSentEvents(HttpRequest req, bool reconnect) async {
    if (!allowServerSentEvents) return _status(req, 400);
    // ignore: close_sinks (event stream stays open; closed in close()/dropTransport())
    final res = req.response;
    res.headers.contentType = ContentType('text', 'event-stream', charset: 'utf-8');
    res.bufferOutput = false;
    _sse = res;
    connectedTransport = 'serverSentEvents';
    if (sendSseInitialized) res.write('data: initialized\n\n');
    if (!reconnect && sendInitMessage) {
      res.write('data: ${jsonEncode(<String, dynamic>{'C': 's-0', 'S': 1, 'M': <dynamic>[]})}\n\n');
    }
  }

  Future<void> _longPoll(HttpRequest req, {required bool initial, required bool reconnect}) async {
    if (!allowLongPolling) return _status(req, 400);
    connectedTransport = 'longPolling';
    if (initial) {
      return _write(req, jsonEncode(<String, dynamic>{'C': 's-0', 'S': sendInitMessage ? 1 : 0, 'M': <dynamic>[]}));
    }
    if (_failPolls > 0) {
      _failPolls--;
      return _status(req, 500);
    }
    if (_lpQueue.isEmpty && !reconnect) {
      final waiter = Completer<void>();
      _lpWaiter = waiter;
      await waiter.future.timeout(
        Duration(milliseconds: (connectionTimeoutSeconds * 1000).round()),
        onTimeout: () {},
      );
      if (_failPolls > 0) {
        _failPolls--;
        return _status(req, 500);
      }
    }
    if (_lpQueue.isEmpty) {
      return _write(req, jsonEncode(<String, dynamic>{'C': 'd-$_cursor', 'M': <dynamic>[]}));
    }
    return _write(req, jsonEncode(_lpQueue.removeAt(0)));
  }

  void _invoke(String data, void Function(String) reply) {
    final inv = jsonDecode(data) as Map<String, dynamic>;
    invocations.add(inv);
    if (silentInvocations) return;
    final id = inv['I'].toString();
    final handler = onInvoke;
    final r = handler == null
        ? Reply()
        : handler(inv['H'] as String, inv['M'] as String, inv['A'] as List<dynamic>, inv['S'] as Map<String, dynamic>?);
    for (final p in r.progress) {
      reply(jsonEncode(<String, dynamic>{
        'I': 'P|$id',
        'P': <String, dynamic>{'I': id, 'D': p},
      }));
    }
    reply(jsonEncode(<String, dynamic>{
      'I': id,
      if (r.error == null) 'R': r.result,
      if (r.error != null) 'E': r.error,
      if (r.error != null && r.isHubError) 'H': true,
      if (r.errorData != null) 'D': r.errorData,
      if (r.state != null) 'S': r.state,
    }));
  }

  Future<void> _write(HttpRequest req, String body) async {
    req.response.headers.contentType = ContentType.json;
    req.response.write(body);
    await req.response.close();
  }

  Future<void> _status(HttpRequest req, int status) async {
    req.response.statusCode = status;
    await req.response.close();
  }
}
