package main

// Modes whose sources are not in this harness copy: only -mode multiq is built.
func absent(m string) { panic("goload: -mode " + m + " is not in this build") }
func runTxnMode(args []string)       { absent("txn") }
func runPrefillMode(args []string)   { absent("prefill") }
func runCloudMode(args []string)     { absent("cloud") }
func runProvisionMode(args []string) { absent("provision") }
func runOrderMode(args []string)     { absent("order") }
func runAppMode(args []string)       { absent("app") }
func runTenantsMode(args []string)   { absent("tenants") }
func runCMMode(args []string)        { absent("cm") }
