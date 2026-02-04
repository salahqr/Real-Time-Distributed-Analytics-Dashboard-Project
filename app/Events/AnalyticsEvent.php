<?php

namespace App\Events;

use Illuminate\Broadcasting\Channel;
use Illuminate\Contracts\Broadcasting\ShouldBroadcast;
use Illuminate\Queue\SerializesModels;

class AnalyticsEvent implements ShouldBroadcast
{
    use SerializesModels;

    public $data;

    /**
     * Create a new event instance.
     */
    public function __construct($data)
    {
        $this->data = $data;
    }

    /**
     * Channel name
     */
    public function broadcastOn()
    {
        return new Channel("analytics-channel");
    }

    /**
     * Event name (frontend listens to this)
     */
    public function broadcastAs()
    {
        return "analytics.new";
    }
}
