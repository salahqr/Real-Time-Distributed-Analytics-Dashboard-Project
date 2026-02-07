<?php

namespace App\Http\Controllers;

use App\Models\User;
use Illuminate\Support\Str;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Hash;
use Illuminate\Support\Facades\Auth;
use Tymon\JWTAuth\Facades\JWTAuth;

class AuthController extends Controller
{
    /**
     * ✅ Register User
     */
    public function register(Request $request)
    {
        $request->validate([
            'name'         => 'required|string|max:255',
            'company_name' => 'nullable|string|max:255',
            'email'        => 'required|email|unique:users',
            'password'     => 'required|min:6|confirmed',
        ]);

        User::create([
            'id'           => Str::uuid(),
            'name'         => $request->name,
            'company_name' => $request->company_name,
            'email'        => $request->email,
            'password'     => Hash::make($request->password),
        ]);

        return response()->json([
            'message' => 'Register success'
        ], 201);
    }

    /**
     * ✅ Login User
     */
    public function login(Request $request)
    {
        $credentials = $request->only('email', 'password');

        if (!Auth::attempt($credentials)) {
            return response()->json([
                'error' => 'Invalid credentials'
            ], 401);
        }

        $user = Auth::user();
        $token = JWTAuth::fromUser($user);

        return response()->json([
            'access_token' => $token,
            'token_type'   => 'bearer',
            'expires_in'   => config('jwt.ttl') * 60
        ]);
    }

    /**
     * ✅ Logout
     */
    public function logout()
    {
        Auth::logout();

        return response()->json([
            'message' => 'Logged out'
        ]);
    }

    /**
     * ✅ Current User Info
     */
    public function me()
    {
        return response()->json(Auth::user());
    }

    /**
     * ✅ Refresh Token
     */
    public function refresh()
    {
        $newToken = JWTAuth::refresh();

        return response()->json([
            'access_token' => $newToken
        ]);
    }
}
