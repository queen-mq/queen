<?php

// Laravel merges this over its own config/cache.php. queue:restart leaves its
// signal in the cache, so every worker has to read the same store: the file
// store under storage/framework/cache, not the framework's default database.
return [
    'default' => 'file',
];
