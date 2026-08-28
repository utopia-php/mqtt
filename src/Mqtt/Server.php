<?php

namespace Utopia\Mqtt;

use Throwable;

class Server
{
    /** @var array<callable> */
    protected array $errorCallbacks = [];

    protected Adapter $adapter;

    public function __construct(Adapter $adapter)
    {
        $this->adapter = $adapter;
    }

    public function start(): void
    {
        try {
            $this->adapter->start();
        } catch (Throwable $error) {
            $this->onError($error, 'start');
        }
    }

    public function shutdown(): void
    {
        try {
            $this->adapter->shutdown();
        } catch (Throwable $error) {
            $this->onError($error, 'shutdown');
        }
    }

    public function send(int $connection, string $message): void
    {
        try {
            $this->adapter->send($connection, $message);
        } catch (Throwable $error) {
            $this->onError($error, 'send');
        }
    }

    public function close(int $connection): void
    {
        try {
            $this->adapter->close($connection);
        } catch (Throwable $error) {
            $this->onError($error, 'close');
        }
    }

    public function onStart(callable $callback): self
    {
        $this->adapter->onStart($callback);

        return $this;
    }

    public function onWorkerStart(callable $callback): self
    {
        $this->adapter->onWorkerStart($callback);

        return $this;
    }

    public function onReceive(callable $callback): self
    {
        $this->adapter->onReceive($callback);

        return $this;
    }

    public function onClose(callable $callback): self
    {
        $this->adapter->onClose($callback);

        return $this;
    }

    /** Register a callback run when a transport operation throws. */
    public function error(callable $callback): self
    {
        $this->errorCallbacks[] = $callback;

        return $this;
    }

    private function onError(Throwable $error, string $action): void
    {
        foreach ($this->errorCallbacks as $callback) {
            $callback($error, $action);
        }
    }
}
