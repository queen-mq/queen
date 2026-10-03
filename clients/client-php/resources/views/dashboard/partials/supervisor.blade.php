{{-- One card per instance; without any, one card reports the supervisor unavailable. --}}
@foreach ($snapshot['instances'] !== [] ? $snapshot['instances'] : [$supervisor] as $instance)
    @include('queen::dashboard.partials.supervisor-instance', ['instanceNumber' => $loop->iteration])
@endforeach
