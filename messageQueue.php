<?php


$redis = new Redis();
$redis->pconnect('localhost', 6379, 1);

$queue = messageQueue::getInstance($redis);

// handler msg
$callback = function ($data) {
    echo 'handle msg is :' . $data['msg'][0] . ' score is : ' . $data['msg'][1] . PHP_EOL;
    return true;
};

// get db_source_data function
$getDb = function () {
    return [
        [messageQueue::get_score() + 1, 1],
        [messageQueue::get_score() + 2, 2],
        [messageQueue::get_score() + 3, 3],
        [messageQueue::get_score() + 4, 4],
        [messageQueue::get_score() + 5, 5],
    ];
};


$queue->setDbData($getDb);
$msg = $queue->dequeue();

var_export($msg);die;


$res = $queue->HandMsg($callback, $msg);


class messageQueue
{

    // 消息优先级 等级为5  1 一般优先 2 初级优先 3中级优先 4 高级优先 5 超级优先
    const GENERAL = 1; // 一般优先级 高于默认队列消息
    const LOWER = 2; // 初级优先 高于一般优先级
    const MIDDLE = 3; // 中级优先 高于初级优先
    const HIGH = 4; // 高级优先 中级优先
    const SUPER = 5; // 超级优先  最高优先级 只有管理员可以设置


    // msg  ack timeout 3600 * 30
    const ACK_TIMEOUT = 108000;

    // 队列名字 默认值 default setQueueName($queueName) 设置
    private $queueName = 'default';
    private $queuePrefix = 'queue';
    private $queueBuffer = 'buffer';

    private $dbDataCallback = null;

    // 队列对象
    private static $instance = null;

    /** @var /Redis $handler */
    private $handler = null;


    private function __construct($redis_handler)
    {
        $this->handler = $redis_handler;
        $this->config();
    }

    public static function getInstance($redis)
    {
        if (null == self::$instance) {
            self::$instance = new messageQueue ($redis);
        }

        return self::$instance;
    }

    /**
     *  优先级实现：
     *      用户发起任务会指定一个权重，权重越大优先级越高
     *      权重相等的任务，时间越早优先级越高
     *
     * @param int $base_score
     * @return float|int
     */
    public static function get_score($base_score = 0)
    {
        return $base_score + (pow((strlen(time()) - 1), 10) / time());
    }

    public function setQueueName(string $queueName)
    {
        $this->queueName = $queueName;

    }

    private function config()
    {
        $queueName = $this->queueName;

        // queue name
        $this->queueName = sprintf('%s:%s:queue', $this->queuePrefix, $queueName);

        // buffer name
        $this->queueBuffer = sprintf('%s:%s:buffer', $this->queuePrefix, $queueName);

    }

    public function getQueueName()
    {
        return $this->queueName;
    }


    protected function getBufferData()
    {

        // pop all
        $luaAllPop = <<<eof
            local key = KEYS[1]
            local start = KEYS[2]
            local stop = KEYS[3]
            local result = redis.call('ZREVRANGEBYSCORE', key, start, stop,'WITHSCORES')
            if result then
                redis.call('ZREMRANGEBYRANK', key, start, stop)
                return result
            else
                return nil
            end
eof;

        $start = time();
        $stop = time() + self::ACK_TIMEOUT;
        $argv = [$this->queueBuffer, $start, $stop];
        $res = $this->popMsg($luaAllPop, $argv, true);
        if (!$res) {
            return [];
        }

        return $res;
    }


    protected function setBufferData(array $data)
    {
        if (!is_array($data) || empty($data)) {
            return false;
        }

        // 设置缓冲区score 为时间戳 用于计算超时 格式[]
        $arr = array_chunk($data, 2);
        $msg = array_map(function ($v) {
            $v[1] = time();
            return array_reverse($v);
        }, $arr);

        // 出队的消息加入缓冲区
        $op = 'zAdd';
        $this->batchHandle($msg, $this->queueBuffer, $op);

        return $msg;
    }


    public function enqueue(array $data = [])
    {

        // 参数为空则获取数据源消息入队
        if (!is_array($data) || empty($data)) {
            $data = $this->getDbData();
        }

        if (empty($data)) {
            return false;
        }

        // redis 批量入队 sort set
        $op = 'zAdd';
        $this->batchHandle($data, $this->queueName, $op);

        return true;
    }

    private function batchHandle($data, $queueName, $op)
    {

        // 展开数组  $param = [0=> score,1=> msg,3=> score ,4=>msg ....];
        $array = iterator_to_array(new RecursiveIteratorIterator(new RecursiveArrayIterator($data)), 0);

        // queueName
        array_unshift($array, $queueName);

        // redis 批量操作 sort set
        call_user_func_array(array($this->handler, $op), $array);
    }

    public function dequeue()
    {

        // 队列为空 则入队一批消息 格式为 ['score'=> '','msg'=> '',]
        $queueSize = $this->getQueueSize($this->queueName);
        if (empty($queueSize)) {
            $this->enqueue();
        }


        // 缓冲区超时消息则入队
        $bufferSize = $this->getQueueSize($this->queueBuffer);
        if (!empty($bufferSize)) {
            $buffer = $this->getBufferData();
            $this->enqueue($buffer);

        }

        // 出队优先级最高的消息
        $msg = $this->popTop();

        // 加入缓冲区
        $this->setBufferData($msg);

        return $msg;

    }

    protected function popTop()
    {
        // 参考 redis 5.0 zpopmax
        $luaMaxPop = <<<'lua'
            local key = KEYS[1];
            local result = redis.call('ZRANGE', key, -1, -1,'WITHSCORES');
            local member = result[1];
            if member then redis.call('ZREM', key, member); 
                return result; 
            else 
                return nil;
            end
lua;

        $argv = [$this->queueName];
        $res = $this->popMsg($luaMaxPop, $argv);
        if (!$res) {
            return [];
        }

        return $res;
    }


    protected function popMsg($LuaScript, $argv, $batch = false)
    {
        $data = [];
        $res = $this->handler->eval($LuaScript, $argv, count($argv));
        if (!$res) {
            return $data;
        }

        //是否批量
        if (!$batch) {
            return $res;
        }

        $msg = array_chunk($res, 2);
        return $msg;
    }

    protected function delBufferData(array $data)
    {
        if (!is_array($data) || empty($data)) {
            return false;
        }

        $msg = array_column($data, '0');
        if (empty($msg)) {
            return false;
        }

        $this->batchHandle($msg, $this->queueBuffer, 'zRem');
    }

    protected function getQueueSize($queueName)
    {
        return $this->handler->zcard($queueName);
    }

    /**
     * @param callable $callback function
     * @param $msg array ['msg'=> '','score'=> '']
     * @param array $param array key=>val
     * @return bool
     */
    public function HandMsg(callable $callback, $msg, array $param = [])
    {
        // 执行用户回调函数
        $param['msg'] = $msg;
        $ack = call_user_func($callback, $param);

        // 应答成功 删除缓冲区消息

        if (true === $ack) {
            $this->delBufferData([$msg]);
            return true;
        }

        return false;
    }


    public function setDbData(callable $callable)
    {
        return $this->dbDataCallback = $callable;
    }

    private function getDbData()
    {
        return call_user_func($this->dbDataCallback);
    }

}

