package localhosttest;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.wowtools.neo4j.rtree.geometry2d.BooleanGeometryDataNodeVisitor;
import org.wowtools.neo4j.rtree.geometry2d.Geometry2dRtreeIntersectsSearcher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DemoProcedures {

    @Context
    public Transaction tx;

    public record EntityContainer(Node node) {
    }

    @Procedure(name = "t1.all", mode = Mode.READ)
    public Stream<EntityContainer> all(@Name("str") String str) {
        System.out.println(str);
        ResourceIterable<Node> iterator = tx.getAllNodes();
        Stream<Node> s = StreamSupport.stream(iterator.spliterator(), false).onClose(() -> {
            iterator.close();
        });
        Stream<EntityContainer> res = s.map(EntityContainer::new);
        return res;
    }

//    @Procedure(name = "t1.geo", mode = Mode.READ)
//    public Stream<EntityContainer> geo(@Name("index") String index, @Name("wkt") String wkt) {
//        Geometry geo;
//        try {
//            geo = new WKTReader().read(wkt);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        MyVisitor myVisitor = new MyVisitor(tx);
//        Geometry2dRtreeIntersectsSearcher searcher = Geometry2dRtreeIntersectsSearcher.get(tx, index);
//        searcher.intersects(geo, tx, myVisitor);
//
//        Supplier<Node> g = new Supplier<>() {
//
//            @Override
//            public Node get() {
//                return null;
//            }
//        };
//        Stream<Node> res = Stream.generate(g).takeWhile((myNode) -> {
//            return !myNode.isLast;
//        });
//        return res.map(EntityContainer::new);
//    }


    private static final class MyVisitor implements BooleanGeometryDataNodeVisitor {
        private boolean running = true;
        private final Transaction tx;
        private final ArrayBlockingQueue<Node> queue = new ArrayBlockingQueue<>(32);

        public MyVisitor(Transaction tx) {
            this.tx = tx;
        }

        @Override
        public boolean visit(String nodeId, Geometry geometry) {
            Node node = tx.getNodeByElementId(nodeId);
            try {
                queue.put(node);
            } catch (InterruptedException e) {
                return false;
            }
            return false;
        }
    }

    @Procedure(name = "t1.sp", mode = Mode.READ)
    public Stream<EntityContainer> sp(@Name("str") String str) {
        System.out.println(str);
        Supplier<MyNode> g = new Supplier<>() {
            int count = 5;

            @Override
            public MyNode get() {
                count--;
                MyNode node = new MyNode(count);
                node.properties.put("name", "name-" + count);
                if (count < 0) {
                    node.isLast = true;
                }
                return node;
            }
        };
        Stream<MyNode> res = Stream.generate(g).takeWhile((myNode) -> {
            return !myNode.isLast;
        });
        return res.map(EntityContainer::new);
    }


    private static final class MyNode implements Node {
        boolean isLast = false;
        private Map<String, Object> properties = new HashMap<>();

        private final int id;

        public MyNode(int id) {
            this.id = id;
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public String getElementId() {
            return "custom" + id;
        }

        @Override
        public boolean hasProperty(String key) {
            return properties.containsKey(key);
        }

        @Override
        public Object getProperty(String key) {
            return properties.get(key);
        }

        @Override
        public Object getProperty(String key, Object defaultValue) {
            return properties.get(key);
        }

        @Override
        public void setProperty(String key, Object value) {

        }

        @Override
        public Object removeProperty(String key) {
            return null;
        }

        @Override
        public Iterable<String> getPropertyKeys() {
            return properties.keySet();
        }

        @Override
        public Map<String, Object> getProperties(String... keys) {
            return properties.entrySet().stream().filter(e -> {
                for (String key : keys) {
                    if (e.getKey().equals(key)) {
                        return true;
                    }
                }
                return false;
            }).collect(HashMap::new, (m, e) -> {
                m.put(e.getKey(), e.getValue());
            }, HashMap::putAll);
        }

        @Override
        public Map<String, Object> getAllProperties() {
            return properties;
        }

        @Override
        public void delete() {

        }

        @Override
        public ResourceIterable<Relationship> getRelationships() {
            return null;
        }

        @Override
        public boolean hasRelationship() {
            return false;
        }

        @Override
        public ResourceIterable<Relationship> getRelationships(RelationshipType... types) {
            return null;
        }

        @Override
        public ResourceIterable<Relationship> getRelationships(Direction direction, RelationshipType... types) {
            return null;
        }

        @Override
        public boolean hasRelationship(RelationshipType... types) {
            return false;
        }

        @Override
        public boolean hasRelationship(Direction direction, RelationshipType... types) {
            return false;
        }

        @Override
        public ResourceIterable<Relationship> getRelationships(Direction dir) {
            return null;
        }

        @Override
        public boolean hasRelationship(Direction dir) {
            return false;
        }

        @Override
        public Relationship getSingleRelationship(RelationshipType type, Direction dir) {
            return null;
        }

        @Override
        public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
            return null;
        }

        @Override
        public Iterable<RelationshipType> getRelationshipTypes() {
            return null;
        }

        @Override
        public int getDegree() {
            return 0;
        }

        @Override
        public int getDegree(RelationshipType type) {
            return 0;
        }

        @Override
        public int getDegree(Direction direction) {
            return 0;
        }

        @Override
        public int getDegree(RelationshipType type, Direction direction) {
            return 0;
        }

        @Override
        public void addLabel(Label label) {

        }

        @Override
        public void removeLabel(Label label) {

        }

        @Override
        public boolean hasLabel(Label label) {
            return false;
        }

        @Override
        public Iterable<Label> getLabels() {
            return List.of(Label.label("hello"));
        }
    }

}
