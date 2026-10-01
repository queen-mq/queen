<?php

namespace App\Notifications;

use App\Support\FailureMatrixLog;
use Illuminate\Notifications\Notification;

/** A custom notification channel: a delivery is one line in the matrix log. */
final class CompatChannel
{
    public function __construct(private FailureMatrixLog $log)
    {
    }

    public function send(object $notifiable, Notification $notification): void
    {
        $message = $notification->toCompat($notifiable);
        $this->log->record($message['run_id'], $message['job_id'], null, 'notified', 'notification');
    }
}
