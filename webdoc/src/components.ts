/**
 * MDX globals registry — components available inside MDX without `import`.
 * Wired via `<Content components={components} />` in `[...slug].astro`.
 * Add new components here as you build (or install) them.
 */

import { Accordion, AccordionContent, AccordionGroup, AccordionTrigger } from "./components/ui/accordion";
import { Aside } from "./components/ui/aside";
import { Badge } from "./components/ui/badge";
import { Card } from "./components/ui/card";
import { CardGrid } from "./components/ui/card-grid";
import Chart from "./components/Chart.astro";
import { Code } from "./components/ui/code";
import { CodeGroup } from "./components/ui/code-group";
import { FileTree } from "./components/ui/file-tree";
import { Frame } from "./components/ui/frame";
import JobLifecycle from "./components/JobLifecycle.astro";
import LeaseTimeline from "./components/LeaseTimeline.astro";
import { LinkButton } from "./components/ui/link-button";
import { LinkCard } from "./components/ui/link-card";
import { PackageManagers } from "./components/ui/package-managers";
import Partition from "./components/Partition.astro";
import PreforkMemory from "./components/PreforkMemory.astro";
import Render from "./components/Render.astro";
import RollingUpdate from "./components/RollingUpdate.astro";
import Screenshot from "./components/Screenshot.astro";
import { Step, Steps } from "./components/ui/steps";
import SupervisorTopology from "./components/SupervisorTopology.astro";
import { Tabs, TabItem } from "./components/ui/tabs";
import WorkerTimeline from "./components/WorkerTimeline.astro";

export const components = {
  Accordion,
  AccordionContent,
  AccordionGroup,
  AccordionTrigger,
  Aside,
  Badge,
  Card,
  CardGrid,
  Chart,
  Code,
  CodeGroup,
  FileTree,
  Frame,
  JobLifecycle,
  LeaseTimeline,
  LinkButton,
  LinkCard,
  PackageManagers,
  Partition,
  PreforkMemory,
  Render,
  RollingUpdate,
  Screenshot,
  Step,
  Steps,
  SupervisorTopology,
  TabItem,
  Tabs,
  WorkerTimeline,
};
