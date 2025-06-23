from collections import defaultdict, deque


class Vertex:
    def __init__(self, id, vtype):
        self.id = id
        self.vtype = vtype
        self.width = dict(Unit=120, Profile=80).get(vtype, None)
        self.height = dict(Unit=60, Profile=80, Node=40).get(vtype, None)
        self.in_edges = []
        self.out_edges = []
        self.pos = (0, 0)

    def adjust_width_for_edges(self):
        if self.vtype == "Node":
            degree = len(self.in_edges) + len(self.out_edges)
            self.width = max(40, 20 + 10 * degree)


class Edge:
    def __init__(self, source, target):
        self.source = source
        self.target = target


class Graph:
    def __init__(self):
        self.vertices = {}
        self.edges = []

    def add_vertex(self, id, vtype):
        self.vertices[id] = Vertex(id, vtype)

    def add_edge(self, source_id, target_id):
        src = self.vertices[source_id]
        tgt = self.vertices[target_id]
        edge = Edge(src, tgt)
        self.edges.append(edge)
        src.out_edges.append(tgt)
        tgt.in_edges.append(src)

    def layout(self):
        layers = defaultdict(list)
        visited = set()
        queue = deque()

        # Initial pass: find roots (nodes with no incoming edges)
        for v in self.vertices.values():
            v.adjust_width_for_edges()
            if not v.in_edges:
                queue.append((v, 0))
                visited.add(v.id)

        # BFS layering by topological structure
        while queue:
            current, depth = queue.popleft()
            layers[depth].append(current)

            for neighbor in current.out_edges:
                if neighbor.id not in visited:
                    queue.append((neighbor, depth + 1))
                    visited.add(neighbor.id)

        # Assign initial positions
        y_spacing = 120
        x_spacing = 160
        positions = {}
        for depth in sorted(layers):
            y = 0
            for v in layers[depth]:
                positions[v.id] = (depth * x_spacing, y + (80 - v.height) / 2)
                v.pos = positions[v.id]
                y += y_spacing

        # Refine for Units (ensure outputs to the right, inputs to the left)
        for v in self.vertices.values():
            if v.vtype == "Unit":
                x, y = v.pos
                for out in v.out_edges:
                    if self.vertices[out.id].pos[0] <= x:
                        # push target to right
                        tx, ty = self.vertices[out.id].pos
                        self.vertices[out.id].pos = (x + v.width + x_spacing, ty)
                for inc in v.in_edges:
                    if self.vertices[inc.id].pos[0] >= x:
                        # push source to left
                        sx, sy = self.vertices[inc.id].pos
                        self.vertices[inc.id].pos = (x - x_spacing - self.vertices[inc.id].width, sy)

        return {vid: v.pos for vid, v in self.vertices.items()}
