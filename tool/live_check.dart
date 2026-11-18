// Live check against a real classic SignalR hub.
//
//   dart run tool/live_check.dart --url https://example.com/myapp --hub markethub \
//     [--methods PriceUpdate,IndexUpdate] [--query session=abc] \
//     [--invoke Handshake --args user1,other] \
//     [--transport auto|webSockets|serverSentEvents|longPolling] \
//     [--seconds 60] [--no-default-path] [--verbose]
//
// `/signalr` is appended to --url unless --no-default-path is given.
// --methods lists the client methods to print; --query is sent as-is on every request.
import 'dart:async';
import 'dart:io';

import 'package:signalr_classic/signalr_classic.dart';

Future<void> main(List<String> argv) async {
  final args = _parse(argv);
  final url = args['url'];
  final hubName = args['hub'];
  if (url == null || url.isEmpty || hubName == null || hubName.isEmpty) {
    stderr.writeln('Usage: dart run tool/live_check.dart --url <url> --hub <hub> [options]');
    exit(64);
  }
  final seconds = int.tryParse(args['seconds'] ?? '') ?? 60;
  final transport = TransportType.values.firstWhere(
    (t) => t.name == (args['transport'] ?? 'auto'),
    orElse: () => TransportType.auto,
  );
  final methods = _list(args['methods']);
  final started = DateTime.now();
  String ts() => '+${DateTime.now().difference(started).inMilliseconds}ms'.padRight(10);

  final connection = HubConnection(
    url,
    options: HubConnectionOptions(
      queryString: args['query'],
      useDefaultPath: !args.containsKey('no-default-path'),
      transport: transport,
      logger:
          args.containsKey('verbose') ? (level, m) => stdout.writeln('${ts()} ${level.name.toUpperCase()} $m') : null,
    ),
  );
  final hub = connection.createHubProxy(hubName);
  for (final method in methods) {
    hub.on(method, (a) {
      final text = a.isEmpty ? '' : a.first.toString();
      final shown = text.length > 160 ? '${text.substring(0, 160)}… (${text.length} chars)' : text;
      stdout.writeln('${ts()} MESSAGE  $method => $shown');
    });
  }
  if (methods.isEmpty) stdout.writeln('No --methods given: hub messages will not be printed.');
  connection.onStateChanged.listen((c) => stdout.writeln('${ts()} STATE    ${c.previous.name} -> ${c.current.name}'
      '${c.current == ConnectionState.connected ? ' (transport: ${connection.transport?.name}, protocol: ${connection.protocolVersion})' : ''}'));
  connection.onConnectionSlow.listen((_) => stdout.writeln('${ts()} SLOW'));
  connection.onError.listen((e) => stdout.writeln('${ts()} ERROR    ${e.kind.name}: ${e.error}'));

  stdout.writeln('Connecting to $url (hub: $hubName, transport: ${transport.name}) for ${seconds}s');
  try {
    await connection.start();
    final invoke = args['invoke'];
    if (invoke != null && invoke.isNotEmpty) {
      final invokeArgs = _list(args['args']);
      stdout.writeln('${ts()} INVOKE   $invoke (${invokeArgs.length} argument(s))');
      final r = await hub.invoke(invoke, invokeArgs, null, const Duration(seconds: 15));
      stdout.writeln('${ts()} RESULT   $invoke => $r');
    }
  } catch (e) {
    stdout.writeln('${ts()} FAILED   $e');
  }
  await Future<void>.delayed(Duration(seconds: seconds));
  await connection.dispose();
  stdout.writeln('${ts()} done');
}

List<String> _list(String? value) => (value ?? '').split(',').map((s) => s.trim()).where((s) => s.isNotEmpty).toList();

Map<String, String> _parse(List<String> argv) {
  final out = <String, String>{};
  for (var i = 0; i < argv.length; i++) {
    final a = argv[i];
    if (!a.startsWith('--')) continue;
    final key = a.substring(2);
    if (i + 1 < argv.length && !argv[i + 1].startsWith('--')) {
      out[key] = argv[++i];
    } else {
      out[key] = '';
    }
  }
  return out;
}
