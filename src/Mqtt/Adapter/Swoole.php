<?php

namespace Utopia\Mqtt\Adapter;

use Swoole\Server;
use Swoole\Server\Port;
use Swoole\WebSocket\Frame;
use Swoole\WebSocket\Server as WebSocketServer;
use Utopia\Mqtt\Adapter;
use Utopia\Mqtt\Packet;

class Swoole extends Adapter
{
    /** Swoole's max_connection cap. */
    private const MAX_CONNECTIONS = 100_000;

    protected Server $server;

    /** Optional WebSocket listener port; null keeps the broker raw-TCP only. */
    private ?int $websocketPort;

    private int $maxPacketLength = 0;

    /** @var array<int, string> per-fd MQTT bytes decoded out of WebSocket messages, awaiting whole packets */
    private array $stream = [];

    /** @var callable|null */
    private $onStart = null;

    /** @var callable|null */
    private $onWorkerStart = null;

    /** @var callable|null */
    private $onReceive = null;

    /** @var callable|null */
    private $onClose = null;

    public function __construct(string $host = '0.0.0.0', int $port = 1883, ?int $websocketPort = null)
    {
        parent::__construct($host, $port);

        $this->websocketPort = $websocketPort;

        if ($websocketPort !== null) {
            // A WebSocket\Server's primary port speaks WebSocket, so it listens there and raw MQTT
            // is added as a TCP listener (see start()). Browsers reach the broker over WebSocket,
            // native clients over MQTT, and both feed the same handlers.
            $this->server = new WebSocketServer($this->host, $websocketPort, SWOOLE_BASE);
        } else {
            $this->server = new Server($this->host, $port, SWOOLE_BASE);
            $this->config['open_mqtt_protocol'] = true;
        }

        $this->config['worker_num'] = 1;
        $this->config['max_connection'] = self::MAX_CONNECTIONS;
    }

    public function start(): void
    {
        // The transport does not keep a connection registry: an application that needs
        // one tracks connections itself (it learns of a client from the CONNECT packet
        // via onReceive, and of a drop via onClose), keyed by whatever domain state it
        // attaches to each fd.
        $this->server->on('close', function (Server $server, int $fd) {
            unset($this->stream[$fd]);
            if ($this->onClose !== null) {
                call_user_func($this->onClose, $fd);
            }
        });

        $this->server->on('receive', function (Server $server, int $fd, int $reactorId, string $data) {
            if ($this->onReceive !== null) {
                call_user_func($this->onReceive, $fd, $data);
            }
        });

        if ($this->websocketPort !== null) {
            $this->listenMqtt();

            // A WebSocket message may hold several or partial MQTT packets, so its payload is
            // buffered and split into whole packets before dispatch, matching the raw-TCP path.
            $this->server->on('message', function (Server $server, Frame $frame) {
                $this->stream[$frame->fd] = ($this->stream[$frame->fd] ?? '') . $frame->data;
                [$packets, $this->stream[$frame->fd]] = Packet::frames($this->stream[$frame->fd]);
                foreach ($packets as $packet) {
                    if ($this->onReceive !== null) {
                        call_user_func($this->onReceive, $frame->fd, $packet);
                    }
                }
            });
        }

        if ($this->onStart !== null) {
            $callback = $this->onStart;
            $this->server->on('start', function () use ($callback) {
                call_user_func($callback);
            });
        }

        if ($this->onWorkerStart !== null) {
            $callback = $this->onWorkerStart;
            $this->server->on('workerStart', function (Server $server, int $workerId) use ($callback) {
                call_user_func($callback, $workerId);
            });
        }

        $this->server->set($this->config);
        $this->server->start();
    }

    /** Add the raw-MQTT TCP listener alongside the WebSocket primary port. */
    private function listenMqtt(): void
    {
        $port = $this->server->addlistener($this->host, $this->port, SWOOLE_SOCK_TCP);
        if (!$port instanceof Port) {
            throw new \RuntimeException('Failed to open the MQTT listener');
        }

        $settings = ['open_mqtt_protocol' => true, 'open_websocket_protocol' => false];
        if ($this->maxPacketLength > 0) {
            $settings['package_max_length'] = $this->maxPacketLength;
        }
        $port->set($settings);
    }

    public function shutdown(): void
    {
        $this->server->shutdown();
    }

    /** Send bytes to a connection, framing them for a WebSocket client and writing raw for TCP. */
    public function send(int $connection, string $message): void
    {
        if ($this->server instanceof WebSocketServer && $this->server->isEstablished($connection)) {
            $this->server->push($connection, $message, WEBSOCKET_OPCODE_BINARY);

            return;
        }

        $this->server->send($connection, $message);
    }

    public function close(int $connection): void
    {
        $this->server->close($connection);
    }

    public function onStart(callable $callback): self
    {
        $this->onStart = $callback;

        return $this;
    }

    public function onWorkerStart(callable $callback): self
    {
        $this->onWorkerStart = $callback;

        return $this;
    }

    public function onReceive(callable $callback): self
    {
        $this->onReceive = $callback;

        return $this;
    }

    public function onClose(callable $callback): self
    {
        $this->onClose = $callback;

        return $this;
    }

    public function setPackageMaxLength(int $bytes): self
    {
        $this->maxPacketLength = $bytes;
        $this->config['package_max_length'] = $bytes;

        return $this;
    }

    public function setWorkerNumber(int $num): self
    {
        $this->config['worker_num'] = $num;

        return $this;
    }

    public function getNative(): Server
    {
        return $this->server;
    }
}
