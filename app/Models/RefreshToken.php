<?php
namespace App\Models;
use Illuminate\Database\Eloquent\Model;

class RefreshToken extends Model {
    protected $fillable = ['user_id','refresh_token','device','ip','expires_at','revoked'];
    public $timestamps = true;
    protected $casts = ['expires_at' => 'datetime','revoked' => 'boolean'];
}
