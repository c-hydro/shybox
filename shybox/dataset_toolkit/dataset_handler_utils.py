class DatasetNamespace:
    """
    Bidirectional namespace:
    - Add key/value pairs dynamically
    - get(x): automatically returns the opposite side (key↔value)
    - Supports attribute access: ns.variable, ns.workflow, etc.
    """

    __slots__ = ("_forward", "_reverse")

    def __init__(self, **pairs):
        object.__setattr__(self, "_forward", {})
        object.__setattr__(self, "_reverse", {})
        for k, v in pairs.items():
            self.add(k, v)

    def add(self, key, value):
        if key in self._forward:
            old_val = self._forward.pop(key)
            self._reverse.pop(old_val, None)
        if value in self._reverse:
            old_key = self._reverse.pop(value)
            self._forward.pop(old_key, None)
        self._forward[key] = value
        self._reverse[value] = key
        return self

    def get(self, x, default=None):
        """Return the paired value (key→value or value→key)."""
        if x in self._forward:
            return self._forward[x]
        if x in self._reverse:
            return self._reverse[x]
        if default is not None:
            return default
        raise KeyError(f"{x!r} not found")

    def __getattr__(self, name):
        if name in self._forward:
            return self._forward[name]
        raise AttributeError(f"{name} not found")

    def __setattr__(self, name, value):
        self.add(name, value)

    def as_dict(self):
        return dict(self._forward)

    def __repr__(self):
        items = ", ".join(f"{k}={v!r}" for k, v in self._forward.items())
        return f"DatasetNamespace({items})"


def make_namespaces(variables, workflows, left_label="variable", right_label="workflow"):
    """
    Create DatasetNamespace(s) from:
      - str, str  → single BiNamespace
      - list[str], list[str]  → list[BiNamespace]
    Returns a DatasetNamespace or a list thereof.
    """
    # Normalize to lists

    if isinstance(variables, list):
        if len(variables) == 1:
            variables = variables[0]
    if isinstance(workflows, list):
        if len(workflows) == 1:
            workflows = workflows[0]

    if isinstance(variables, str) and isinstance(workflows, str):
        return DatasetNamespace(**{left_label: variables, right_label: workflows})

    if isinstance(variables, (list, tuple)) and isinstance(workflows, (list, tuple)):
        if len(variables) != len(workflows):
            raise ValueError("variables and workflows must have the same length")
        return [DatasetNamespace(**{left_label: v, right_label: w}) for v, w in zip(variables, workflows)]

    raise TypeError("Expected (str, str) or (list[str], list[str]) for variables/workflows")
