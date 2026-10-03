<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Notifications\Notifiable;

/** A row of `compat_users`, which `bench:compat setup` creates: a notifiable, and a model that jobs carry. */
final class CompatUser extends Model
{
    use Notifiable;

    public $timestamps = false;

    protected $fillable = ['name'];
}
