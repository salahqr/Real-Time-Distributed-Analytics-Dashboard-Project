<?php
namespace App\Http\Controllers;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Password;
use Illuminate\Support\Str;
use Illuminate\Auth\Events\PasswordReset;
use App\Models\User;
use Illuminate\Support\Facades\Hash;

class PasswordController extends Controller {
    public function sendResetLink(Request $req) {
        $req->validate(['email'=>'required|email']);
        $status = Password::sendResetLink($req->only('email'));
        return $status == Password::RESET_LINK_SENT
            ? response()->json(['message'=>'Reset link sent'])
            : response()->json(['message'=>'Unable to send reset link'],500);
    }

    public function reset(Request $req) {
        $req->validate([
            'token'=>'required',
            'email'=>'required|email',
            'password'=>'required|min:8|confirmed'
        ]);
        $status = Password::reset($req->only('email','password','password_confirmation','token'),
            function($user,$password){
                $user->forceFill(['password'=>Hash::make($password)])->save();
                event(new PasswordReset($user));
            });
        return $status == Password::PASSWORD_RESET
            ? response()->json(['message'=>'Password reset'])
            : response()->json(['message'=>'Reset failed'],500);
    }
}
