<?php

namespace App\Http\Middleware;

use Closure;
use Illuminate\Http\Request;

class CheckRole
{
    public function handle(Request $request, Closure $next, string $role)
    {
        // ----------------------------------------------------
        // التعديل هنا: تحديد الـ Guard 'api' بشكل صريح
        // ----------------------------------------------------
        $user = auth('api')->user();

        if (!$user || ($user->role !== $role)) {
            return response()->json([
                'message' => 'Forbidden'
            ], 403);
        }

        return $next($request);
    }
}
