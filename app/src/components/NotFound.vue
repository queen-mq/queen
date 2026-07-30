<template>
  <div class="empty-page">
    <div class="empty-code">404</div>
    <h2>No such page</h2>
    <p><code>{{ $route.fullPath }}</code> is not part of this dashboard.</p>
    <router-link to="/" class="empty-action">Back to the dashboard</router-link>
  </div>
</template>
