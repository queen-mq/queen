<?php

// Each worker writes its log lines to its own output, which the examples keep
// in a file per worker under storage/examples/.
return [
    'default' => 'stderr',
];
