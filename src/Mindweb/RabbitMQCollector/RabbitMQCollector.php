<?php
namespace Mindweb\RabbitMQCollector;

use Mindweb\Modifier;
use Mindweb\Forwarder;
use Mindweb\Collector;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;

class RabbitMQCollector implements Collector\Collector
{
    /**
     * @var string
     */
    private $host;

    /**
     * @var string
     */
    private $port;

    /**
     * @var string
     */
    private $user;

    /**
     * @var string
     */
    private $password;

    /**
     * @var string
     */
    private $vhost;

    /**
     * @var bool
     */
    private $insist;

    /**
     * @var string
     */
    private $login_method;

    /**
     * @var null|string
     */
    private $login_response;

    /**
     * @var string
     */
    private $locale;

    /**
     * @var int
     */
    private $connection_timeout;

    /**
     * @var int
     */
    private $read_write_timeout;

    /**
     * @var string
     */
    private $queue;

    /**
     * @var int
     */
    private $numberOfWorkers;

    /**
     * @param array $configuration
     */
    public function __construct(array $configuration)
    {
        $this->host = !empty($configuration['host']) ? $configuration['host'] : 'localhost';
        $this->port = !empty($configuration['port']) ? $configuration['port'] : '5672';
        $this->user = !empty($configuration['user']) ? $configuration['user'] : 'guest';
        $this->password = !empty($configuration['password']) ? $configuration['password'] : 'guest';
        $this->vhost = !empty($configuration['vhost']) ? $configuration['vhost'] : '/';
        $this->insist = !empty($configuration['insist']) ? $configuration['insist'] : false;
        $this->login_method = !empty($configuration['login_method']) ? $configuration['login_method'] : 'AMQPLAIN';
        $this->login_response = !empty($configuration['login_response']) ? $configuration['login_response'] : null;
        $this->locale = !empty($configuration['locale']) ? $configuration['locale'] : 'en_US';
        $this->connection_timeout = !empty($configuration['connection_timeout']) ? $configuration['connection_timeout'] : 3;
        $this->read_write_timeout = !empty($configuration['read_write_timeout']) ? $configuration['read_write_timeout'] : 3;
        $this->queue = !empty($configuration['exchange']) ? $configuration['exchange'] : 'analytics.actions';
        $this->numberOfWorkers = !empty($configuration['numberOfWorkers']) ? $configuration['numberOfWorkers'] : 1;
    }

    /**
     * @param Modifier\Collection $modifiers
     * @param Forwarder\Collection $forwarders
     */
    public function run(Modifier\Collection $modifiers, Forwarder\Collection $forwarders)
    {
        if ($this->numberOfWorkers === 1) {
            $this->runWorker($modifiers, $forwarders);
        } elseif ($this->numberOfWorkers > 1) {
            for ($i = 0; $i < $this->numberOfWorkers; ++$i) {
                $pid = pcntl_fork();
                if ($pid == -1) {
                    throw new \RuntimeException('Could not fork.');
                } else if ($pid) {
                    throw new \RuntimeException('Could not fork. (zombie children)');
                } else {
                    $this->runWorker($modifiers, $forwarders);
                }
            }
        }
    }

    /**
     * @param Modifier\Collection $modifiers
     * @param Forwarder\Collection $forwarders
     */
    private function runWorker(Modifier\Collection $modifiers, Forwarder\Collection $forwarders)
    {
        $channel = $this->getChannel();

        $callback = function($msg) use ($modifiers, $forwarders) {
            $forwarders->forward(
                $modifiers->modify(
                    json_decode($msg->body, true)
                )
            );
        };

        $channel->basic_consume(
            $this->queue,
            '',
            false,
            true,
            false,
            false,
            $callback
        );

        while(count($channel->callbacks)) {
            $channel->wait();
        }
    }

    /**
     * @return AMQPChannel
     */
    private function getChannel()
    {
        $connection = new AMQPStreamConnection(
            $this->host,
            $this->port,
            $this->user,
            $this->password,
            $this->vhost,
            $this->insist,
            $this->login_method,
            $this->login_response,
            $this->locale,
            $this->connection_timeout,
            $this->read_write_timeout
        );

        return $connection->channel();
    }
}