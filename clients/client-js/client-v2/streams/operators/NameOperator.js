/**
 * NameOperator — `.name('perTenant')`: the name of the stateful step before
 * it, the one a live read asks for (`handle.state('perTenant')`,
 * `Stream.view(queen, queryId).get('perTenant', key)`). Unnamed steps are
 * `step1`, `step2`, ... in chain order.
 */
export class NameOperator {
  constructor(name) {
    if (typeof name !== 'string' || !/^[A-Za-z0-9_.-]{1,64}$/.test(name)) {
      throw new Error(`.name(): a step name is 1 to 64 letters, digits, '_', '.' or '-', got ${JSON.stringify(name)}`)
    }
    this.kind = 'name'
    this.name = name
    this.config = { kind: 'name', name }
  }
}
