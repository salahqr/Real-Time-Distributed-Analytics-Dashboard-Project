<?php

namespace App\Models;

use Illuminate\Foundation\Auth\User as Authenticatable;
use Illuminate\Notifications\Notifiable;
use Tymon\JWTAuth\Contracts\JWTSubject;
use Illuminate\Contracts\Auth\MustVerifyEmail;

class User extends Authenticatable implements JWTSubject, MustVerifyEmail
{
    use Notifiable;

    /**
     * لأننا نستخدم UUID بدل auto increment
     */
    public $incrementing = false;
    protected $keyType = 'string';

    /**
     * الحقول المسموح تعبئتها
     */
    protected $fillable = [
        'id',
        'name',
        'company_name',
        'email',
        'password'
    ];

    /**
     * الحقول المخفية
     */
    protected $hidden = [
        'password'
    ];

    /**
     * JWT Identifier
     */
    public function getJWTIdentifier()
    {
        return $this->getKey();
    }

    /**
     * JWT Custom Claims
     */
    public function getJWTCustomClaims()
    {
        return [];
    }
}
