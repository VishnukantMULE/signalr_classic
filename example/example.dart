// ignore_for_file: avoid_print
import 'package:signalr_classic/signalr_classic.dart';

Future<void> main() async {
  final connection = HubConnection(
    'https://example.com/myapp', // `/signalr` is appended
    options: const HubConnectionOptions(
      queryString: 'session=<session>',
      transport: TransportType.auto, // WebSockets -> SSE -> Long Polling
    ),
  );

  // Hub proxies must be created before start().
  final hub = connection.createHubProxy('markethub');
  hub.on('PriceUpdate', (args) => print('price update: ${args.first}'));

  connection.onStateChanged.listen((c) => print('state: ${c.previous.name} -> ${c.current.name}'));
  connection.onConnectionSlow.listen((_) => print('connection slow'));
  connection.onClosed.listen((error) => print('closed${error == null ? '' : ': $error'}'));

  try {
    await connection.start();
    final result = await hub.invoke('Handshake', <dynamic>['<user>']);
    print('Handshake => $result');
  } on SignalRException catch (e) {
    print('failed: $e');
  } finally {
    await connection.dispose();
  }
}
