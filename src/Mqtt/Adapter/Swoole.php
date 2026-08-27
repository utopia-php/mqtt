<?php

namespace Utopia\Mqtt\Adapter;

use Swoole\Server;
use Utopia\Mqtt\Adapter;

class Swoole extends Adapter
{
    protected Server $server;

    /** @var array<int, bool> */
    private static array $connections = [];

    public function __construct(string $host = '0.0.0.0', int $port = 1883)
    {
        parent::__construct($host, $port);

        $this->server = new Server($this->host, $this->port, SWOOLE_BASE);

        $this->config['open_mqtt_protocol'] = true;
        $this->config['worker_num'] = 1;
    }

    public function start(): void
    {
        // Track every connection from the moment it is established, so getConnections()
        // includes clients that have not sent a packet yet.
        $this->server->on('connect', function (Server $server, int $fd) {
            self::$connections[$fd] = true;
        });

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
        $this->server->on('start', function () use ($callback) {
            call_user_func($callback);
        });

        return $this;
    }

    public function onWorkerStart(callable $callback): self
    {
        $this->server->on('workerStart', function (Server $server, int $workerId) use ($callback) {
            call_user_func($callback, $workerId);
        });

        return $this;
    }

    public function onReceive(callable $callback): self
    {
        $this->server->on('receive', function (Server $server, int $fd, int $reactorId, string $data) use ($callback) {
            call_user_func($callback, $fd, $data);
        });

        return $this;
    }

    public function onClose(callable $callback): self
    {
        $this->server->on('close', function (Server $server, int $fd) use ($callback) {
            unset(self::$connections[$fd]);

            call_user_func($callback, $fd);
        });

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
        return array_keys(self::$connections);
    }
}
