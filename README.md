# Utopia MQTT

[![Discord](https://img.shields.io/discord/564160730845151244?label=discord)](https://appwrite.io/discord)

Utopia MQTT is a simple and lite abstraction layer for building MQTT 5.0 brokers and clients: a swappable transport plus a standard MQTT packet codec. It aims to be as simple and easy to learn and use as possible. This library is maintained by the [Appwrite team](https://appwrite.io).

It gives you two layers and nothing application specific on top:

- **Transport** — accept connections, frame MQTT packets off the TCP stream, hand you raw bytes per connection (`Adapter`, `Server`, `Client`).
- **Protocol codec** — decode a framed packet (`Packet`), encode control packets per version (`Packet\V3`, `Packet\V5`), and model the v5 property block as objects (`Property`, `Properties`).

Broker behaviour on top of these — subscription matching, QoS bookkeeping, authentication — lives in your application, so the networking runtime (Swoole today, others later) stays swappable.

Although this library is part of the [Utopia Framework](https://github.com/utopia-php/framework) project, it is dependency free and can be used as standalone with any other PHP project or framework.

## Getting Started

Install using composer:

```bash
composer require utopia-php/mqtt
```

Init in your application:

```php
<?php

require_once __DIR__ . '/../vendor/autoload.php';

use Utopia\Mqtt\Adapter;
use Utopia\Mqtt\Server;

$adapter = new Adapter\Swoole(host: '0.0.0.0', port: 1883);
$adapter->setPackageMaxLength(64000);

$server = new Server($adapter);

$server->error(function (\Throwable $error, string $action) {
    echo "Transport error during {$action}: {$error->getMessage()}\n";
});

$server->onStart(function () {
    echo "MQTT broker started\n";
});

$server->onReceive(function (int $connection, string $data) use ($server) {
    // $data is one framed MQTT packet — decode it with the codec.
    $packet = Utopia\Mqtt\Packet::parse($data);

    if ($packet->type === Utopia\Mqtt\Packet::PINGREQ) {
        $server->send($connection, Utopia\Mqtt\Packet::pingresp());
    }

    echo "{$packet->name()} from {$connection}\n";
});

$server->onClose(function (int $connection) {
    echo "Connection {$connection} closed\n";
});

$server->start();
```

## The abstraction

- **`Adapter`** — the transport contract: `start` / `shutdown`, `send` / `close`, the `onStart` / `onWorkerStart` / `onReceive` / `onClose` lifecycle hooks, and config setters (`setPackageMaxLength`, `setWorkerNumber`). `onReceive` normalizes every runtime to `(int $connection, string $data)`.
- **`Adapter\Swoole`** — a Swoole implementation. Uses `open_mqtt_protocol` so the runtime frames one MQTT packet per `onReceive`.
- **`Server`** — a thin wrapper over an `Adapter` that delegates the hooks and the `send` / `close` writes, catching transport errors and routing them to callbacks registered with `error()` instead of letting them escape the event loop.
- **`Client`** — a lite broker client over TCP/TLS (`mqtt://` / `mqtts://`) that frames packets the same way; `connect`, `send`, `receive` / `listen`, with `onOpen` / `onReceive` / `onClose` / `onError`.
- **`Packet`** — a decoded packet (`type`, `flags`, `qos()`, `body`) plus the version-agnostic wire primitives (`parse`, `encodeLength` / `encodeString`, `pingreq` / `pingresp`).
- **`Packet\V3`** and **`Packet\V5`** — the per-version encoders. v3.1.1 has return codes and no properties; v5 has reason codes and a property block (`connack`, `publish`, `puback`, `suback`, `unsuback`, `disconnect`, and — v5 only — `auth`).
- **`Property`** / **`Properties`** — the MQTT 5.0 property block as objects: `new Property(Property::SESSION_EXPIRY_INTERVAL, 60)` knows its wire type, and a `Properties` collection encodes/parses the block (`(new Properties())->add(new Property(Property::USER, ['projectId' => 'p1']))`).

## Encoding packets (v3.1.1 and v5)

The version is the class you call. A client declares its protocol level in the CONNECT — level `4` is MQTT 3.1.1, level `5` is MQTT 5.0 — so read it once and reply with the matching encoder:

```php
use Utopia\Mqtt\Packet;

$packet = Packet::parse($data);      // a CONNECT
$offset = 0;
[, $offset] = Packet::readString($packet->body, $offset); // "MQTT"
$level = ord($packet->body[$offset]);                     // 4 = v3.1.1, 5 = v5
```

### MQTT 3.1.1 — `Packet\V3`

No property block; CONNACK uses a return code and SUBACK a granted-QoS (or failure) byte.

```php
use Utopia\Mqtt\Packet;
use Utopia\Mqtt\Packet\V3;

$server->send($fd, V3::connack(V3::RETURN_ACCEPTED));            // accept
$server->send($fd, V3::connack(V3::RETURN_NOT_AUTHORIZED));      // refuse

$server->send($fd, V3::publish('sport/tennis', 'hello', qos: 1, packetId: 42));

$server->send($fd, V3::suback($packetId, chr(Packet::QOS_1)));         // granted QoS 1
$server->send($fd, V3::suback($packetId, chr(V3::SUBSCRIBE_FAILURE))); // filter refused

$server->send($fd, V3::puback($packetId));
```

### MQTT 5.0 — `Packet\V5`

Every acknowledgement carries a reason code, and a `Properties` block can ride along.

```php
use Utopia\Mqtt\Packet;
use Utopia\Mqtt\Packet\V5;
use Utopia\Mqtt\Properties;
use Utopia\Mqtt\Property;

$server->send($fd, V5::connack(V5::REASON_SUCCESS));         // accept
$server->send($fd, V5::connack(V5::REASON_NOT_AUTHORIZED));  // refuse

// A PUBLISH carrying properties (content type + a User Property) before the payload.
$properties = (new Properties())
    ->add(new Property(Property::CONTENT_TYPE, 'application/json'))
    ->add(new Property(Property::USER, ['event' => 'push']));
$server->send($fd, V5::publish('appwrite/push/user-1', $json, qos: 1, packetId: 42, properties: $properties));

$server->send($fd, V5::suback($packetId, chr(V5::REASON_SUCCESS)));       // granted
$server->send($fd, V5::suback($packetId, chr(V5::REASON_NOT_AUTHORIZED))); // denied

$server->send($fd, V5::puback($packetId));
```

Read a v5 property block off any packet with `Properties::parse($body, $offset)`, which returns the collection and the offset just past the block:

```php
[$properties, $offset] = Properties::parse($packet->body, $offset);
$properties->get(Property::CONTENT_TYPE); // 'application/json'
$properties->user();                      // ['event' => 'push']
```

### Enhanced authentication (v5)

v5 enhanced auth is carried entirely in the property block: Authentication Method, Authentication Data, and any metadata as User Properties. The server reads them off the CONNECT and answers with a CONNACK (done) or an AUTH (continue).

```php
use Utopia\Mqtt\Packet;
use Utopia\Mqtt\Packet\V5;
use Utopia\Mqtt\Properties;
use Utopia\Mqtt\Property;

// Server: read the auth method, data and metadata out of the CONNECT.
$body = $packet->body;
$offset = 0;
[, $offset] = Packet::readString($body, $offset); // "MQTT"
$offset += 1;                                      // protocol level (5)
$offset += 1;                                      // connect flags
$offset += 2;                                      // keep alive
[$properties, $offset] = Properties::parse($body, $offset);

$method   = $properties->get(Property::AUTHENTICATION_METHOD); // 'appwrite-jwt'
$secret   = $properties->get(Property::AUTHENTICATION_DATA);   // the token / secret
$metadata = $properties->user();                              // ['projectId' => 'p1', ...]

// Server: answer.
if ($authenticated) {
    $server->send($fd, V5::connack(V5::REASON_SUCCESS));
} else {
    $server->send($fd, V5::connack(V5::REASON_NOT_AUTHORIZED));
    $server->close($fd);
}

// Server: or ask for another round (multi-step auth).
$server->send($fd, V5::auth(V5::AUTH_CONTINUE, (new Properties())
    ->add(new Property(Property::AUTHENTICATION_METHOD, $method))
    ->add(new Property(Property::AUTHENTICATION_DATA, $challenge))));
```

Re-authenticating mid-connection is an AUTH packet with a fresh credential and metadata — the same `Properties` on the wire, sent by either side:

```php
$reauth = V5::auth(V5::AUTH_REAUTH, (new Properties())
    ->add(new Property(Property::AUTHENTICATION_METHOD, 'appwrite-jwt'))
    ->add(new Property(Property::AUTHENTICATION_DATA, $freshToken))
    ->add(new Property(Property::USER, ['projectId' => 'p1'])));

$client->send($reauth);

// The receiver: reason code first, then the property block.
$packet = Packet::parse($data);           // $packet->type === Packet::AUTH
$reason = ord($packet->body[0]);          // 0x19 = re-authenticate
[$properties] = Properties::parse($packet->body, 1);
$properties->get(Property::AUTHENTICATION_DATA); // the fresh token
```

(MQTT 3.1.1 has no AUTH packet and no properties, so enhanced auth is v5 only — a v3 client authenticates with the username/password fields of the CONNECT payload.)

## System Requirements

Utopia MQTT requires PHP 8.1 or later. We recommend using the latest PHP version whenever possible. The `Adapter\Swoole` implementation additionally requires the Swoole extension.

## Tests

To run all unit tests, use the following Composer command:

```bash
composer test
```

## Copyright and license

The MIT License (MIT) [http://www.opensource.org/licenses/mit-license.php](http://www.opensource.org/licenses/mit-license.php)
