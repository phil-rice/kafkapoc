export type NameAnd<T> = Record<string, T>;

export type ExemplarTree<Exemplar, Input> = {
    aspect: keyof Input;
    children?: NameAnd<ExemplarTree<Exemplar, Input>>;
    exemplars?: Exemplar[];
};

/** Typeclass: map an exemplar to Input, and define the question order */
export type ExemplarTC<Exemplar, Input extends Record<string, unknown>> = {
    input: (e: Exemplar) => Input;
    decisionTreeOrder: (keyof Input)[];
};

function nextAspect<Input>(
    current: keyof Input,
    order: (keyof Input)[]
): keyof Input | undefined {
    const i = order.indexOf(current);
    return i >= 0 && i + 1 < order.length ? order[i + 1] : undefined;
}

function ensureChildren<Ex, Input>(
    n: ExemplarTree<Ex, Input>
): NameAnd<ExemplarTree<Ex, Input>> {
    return (n.children ??= {});
}

function ensureExemplars<Ex, Input>(n: ExemplarTree<Ex, Input>): Ex[] {
    return (n.exemplars ??= []);
}

/** Finds the deepest matching node for the given Input. */
function findLeafNodeForInput<Ex, Input extends Record<string, unknown>>(
    tree: ExemplarTree<Ex, Input>,
    data: Input
): ExemplarTree<Ex, Input> {
    const key = String(data[tree.aspect]);
    const next = tree.children?.[key];
    return next ? findLeafNodeForInput(next, data) : tree;
}

/** Finds the deepest matching node for the given exemplar. */
export function findLeafNodeForExemplar<
    Ex,
    Input extends Record<string, unknown>
>(
    tree: ExemplarTree<Ex, Input>,
    exemplar: Ex,
    tc: ExemplarTC<Ex, Input>
): ExemplarTree<Ex, Input> {
    return findLeafNodeForInput(tree, tc.input(exemplar));
}

/** Mutating add: places exemplar at the right spot, creating a child if we can move one step deeper. */
export function addExemplarToTree<
    Ex,
    Input extends Record<string, unknown>
>(
    tree: ExemplarTree<Ex, Input>,
    exemplar: Ex,
    tc: ExemplarTC<Ex, Input>
): void {
    const { input, decisionTreeOrder } = tc;
    const data = input(exemplar);

    // Root special case: empty root ⇒ just attach exemplar
    if ((tree.exemplars?.length ?? 0) === 0 && !tree.children) {
        tree.exemplars = [exemplar];
        return;
    }

    const leaf = findLeafNodeForInput(tree, data);

    const raw = data[leaf.aspect];
    const hasValue = raw !== undefined && raw !== null; // avoid "undefined"/"null" buckets
    const valueKey = hasValue ? String(raw) : undefined;
    const nxt = nextAspect<Input>(leaf.aspect, decisionTreeOrder);

    // Create a child only if we have a real value, a next aspect, and no existing child
    if (hasValue && nxt !== undefined) {
        const children = ensureChildren(leaf);
        if (!children[valueKey!]) {
            children[valueKey!] = { aspect: nxt };
        }
    }

    // Push the current exemplar directly to the right place
    const dest =
        hasValue && leaf.children ? leaf.children[valueKey!] : undefined;
    ensureExemplars(dest ?? leaf).push(exemplar);

    // Optional: tidy previously attached exemplars down one level
    pushExemplarsDownIfNeeded(leaf, tc);
}

function pushExemplarsDownIfNeeded<
    Ex,
    Input extends Record<string, unknown>
>(
    node: ExemplarTree<Ex, Input>,
    tc: ExemplarTC<Ex, Input>
): void {
    if (!node.children) return;

    const { input } = tc;

    const stay: Ex[] = [];
    for (const ex of node.exemplars ?? []) {
        const raw = input(ex)[node.aspect];
        if (raw === undefined || raw === null) {
            stay.push(ex); // don't create null/undefined buckets
            continue;
        }
        const k = String(raw);
        const child = node.children[k];
        if (child) ensureExemplars(child).push(ex);
        else stay.push(ex);
    }
    node.exemplars = stay;

    for (const k of Object.keys(node.children)) {
        pushExemplarsDownIfNeeded(node.children[k], tc);
    }
}
