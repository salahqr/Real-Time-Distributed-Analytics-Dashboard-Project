<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Password;
use Illuminate\Support\Facades\Hash;
use Illuminate\Auth\Events\PasswordReset;
use App\Models\User;

class PasswordResetController extends Controller
{
    /**
     * إرسال رابط إعادة تعيين كلمة المرور إلى الإيميل
     */
    public function sendResetLink(Request $request)
    {
        // 1) التحقق من الإيميل
        $request->validate([
            'email' => 'required|email'
        ]);

        // 2) إرسال رابط إعادة التعيين
        $status = Password::sendResetLink(
            $request->only('email')
        );

        // 3) الرد
        return $status === Password::RESET_LINK_SENT
            ? response()->json([
                'message' => 'Reset link sent to your email'
            ])
            : response()->json([
                'error' => 'Unable to send reset link'
            ], 500);
    }

    /**
     * إعادة تعيين كلمة المرور
     */
    public function resetPassword(Request $request)
    {
        // 1) التحقق من البيانات
        $request->validate([
            'token' => 'required',
            'email' => 'required|email',
            'password' => 'required|min:6|confirmed',
        ]);

        // 2) إعادة تعيين كلمة المرور
        $status = Password::reset(
            $request->only(
                'email',
                'password',
                'password_confirmation',
                'token'
            ),
            function (User $user, string $password) {
                $user->password = Hash::make($password);
                $user->save();

                event(new PasswordReset($user));
            }
        );

        // 3) الرد
        return $status === Password::PASSWORD_RESET
            ? response()->json([
                'message' => 'Password reset successfully'
            ])
            : response()->json([
                'error' => 'Invalid token or email'
            ], 400);
    }
}
