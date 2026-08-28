<?php

namespace Utopia\Mqtt\Adapter;

use Swoole\Server;
use Utopia\Mqtt\Adapter;

class Swoole extends Adapter
{
    /** Upper bound on tracked connections; also Swoole's own max_connection. */
    private const MAX_CONNECTIONS = 100_000;

    protected Server $server;

    /**
     * Local connection registry. The adapter runs a single worker by default, so a
     * plain array is a complete view; an app that scales workers keeps its own
     * authoritative connection state (as the Appwrite MQTT adapter does).
     *
     * @var array<int, bool>
     */
    protected array $connections = [];

    /** @var callable|null */
    private $onStart = null;

    /** @var callable|null */
    private $onWorkerStart = null;

    /** @var callable|null */
    private $onReceive = null;

    /** @var callable|null */
    private $onClose = null;

    public function __construct(string $host = '0.0.0.0', int $port = 1883)
    {
        parent::__construct($host, $port);

        $this->server = new Server($this->host, $this->port, SWOOLE_BASE);

        $this->config['open_mqtt_protocol'] = true;
        $this->config['worker_num'] = 1;
        $this->config['max_connection'] = self::MAX_CONNECTIONS;
    }

    public function start(): void
    {
        // The connection registry is maintained here, independent of the application's
        // callbacks: connect adds and close removes, so getConnections() is always
        // accurate even when onClose() is never registered. The application callbacks,
        // when set, run alongside.
        $this->server->on('connect', function (Server $server, int $fd) {
            $this->connections[$fd] = true;
        });

        $this->server->on('close', function (Server $server, int $fd) {
            unset($this->connections[$fd]);

            if ($this->onClose !== null) {
                call_user_func($this->onClose, $fd);
            }
        });

        $this->server->on('receive', function (Server $server, int $fd, int $reactorId, string $data) {
            if ($this->onReceive !== null) {
                call_user_func($this->onReceive, $fd, $data);
            }
        });

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

    public function shutdown(): void
    {
        $this->server->shutdown();
    }

    public function send(int $connection, string $message): void
    {
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

    public function getConnections(): array
    {
        return array_keys($this->connections);
    }
}
