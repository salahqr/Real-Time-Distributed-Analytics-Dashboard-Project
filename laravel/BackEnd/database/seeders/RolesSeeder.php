<?php
namespace Database\Seeders;
use Illuminate\Database\Seeder;
use App\Models\Role;

class RolesSeeder extends Seeder {
    public function run() {
        $roles = [
            ['name'=>'admin','description'=>'Full access'],
            ['name'=>'user','description'=>'Normal user'],
            ['name'=>'analyst','description'=>'Analytics viewer']
        ];
        foreach($roles as $r) Role::firstOrCreate(['name'=>$r['name']], $r);
    }
}
