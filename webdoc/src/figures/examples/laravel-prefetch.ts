import { bar } from "@/lib/figure-spec";

// examples/laravel/prefetch.mdx, phase b: the jobs of each pop of one worker
// with prefetch 'auto', on a backlog of 200 jobs of 10 ms, from the run the
// page prints.
export default bar({
  alt: "The jobs each pop took, for one worker with prefetch auto on a backlog of 200 jobs of 10 ms, 21 pops in order: 1, 1, 2, 2, 4, 4, 8, 8, 13, 13, 16, 16, 16, 14, then 12 six times, and 10.",
  caption: "The batch doubles after every two full pops until it holds about 250 ms of work. From there it follows the measured job time, between 12 and the ceiling of 16. The last pop took the last 10 jobs.",
  source: "examples/apps/laravel, php artisan example:prefetch",
  categories: Array.from({ length: 21 }, (_, i) => `pop ${i + 1}`),
  series: [
    {
      label: "jobs in the pop",
      values: [1, 1, 2, 2, 4, 4, 8, 8, 13, 13, 16, 16, 16, 14, 12, 12, 12, 12, 12, 12, 10],
    },
  ],
  x: { label: "jobs in the pop", min: 0, max: 16, ticks: [0, 4, 8, 12, 16], format: "int" },
});
