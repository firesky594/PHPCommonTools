<?php

declare(strict_types=1);

// 仅供独立包测试，项目Composer自动加载不受影响。
spl_autoload_register(static function (string $class): void {
    $prefix = 'Firesky594\\PhpCommonTools\\';
    if (str_starts_with($class, $prefix)) {
        $file = dirname(__DIR__) . '/src/' . str_replace('\\', '/', substr($class, strlen($prefix))) . '.php';
        if (is_file($file)) {
            require $file;
        }
    }
});
