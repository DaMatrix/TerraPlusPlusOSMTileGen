#include "tpposmtilegen_common.h"

#include <osmium/area/assembler.hpp>
#include <osmium/builder/attr.hpp>

using namespace osmium::builder::attr;

static jclass c_area;
static jclass c_shape;
static jclass c_point;
static jclass c_point_array;

static jmethodID ctor_area;
static jmethodID ctor_shape;
static jmethodID ctor_point;

static osmium::area::Assembler::config_type assembler_config;

static jobject toShape(JNIEnv *env, jobjectArray outerLoop, std::vector<jobjectArray> &innerLoops) {
    pushLocalFrame(env, 2);

    jobjectArray innerLoopsArray = env->NewObjectArray(innerLoops.size(), c_point_array, nullptr);
    for (int i = 0; i < innerLoops.size(); i++) {
        env->SetObjectArrayElement(innerLoopsArray, i, innerLoops[i]);
    }

    return popLocalFrame(env, env->NewObject(c_shape, ctor_shape, outerLoop, innerLoopsArray));
}

static jobjectArray toPointArray(JNIEnv *env, const osmium::NodeRefList &nodes) {
    pushLocalFrame(env, 2);

    jobjectArray array = env->NewObjectArray(nodes.size(), c_point, nullptr);
    for (int i = 0; i < nodes.size(); i++) {
        jobject point = env->NewObject(c_point, ctor_point, nodes[i].x(), nodes[i].y());
        env->SetObjectArrayElement(array, i, point);
        env->DeleteLocalRef(point);
    }
    return popLocalFrame(env, array);
}

jobject toArea(JNIEnv *env, const osmium::Area &area) {
    jint num_outer_loops = 1;
    jint num_inner_loops = 0;
    jint num_shapes = 0;
    for (const auto &item : area) {
        if (item.type() == osmium::item_type::outer_ring) {
            num_shapes++;
        } else if (item.type() == osmium::item_type::inner_ring) {
            num_inner_loops++;
        }
    }

    pushLocalFrame(env, num_inner_loops + num_inner_loops + num_shapes + 1 + 1);

    std::vector<jobject> shapes;

    jobjectArray outerLoop;
    std::vector<jobjectArray> innerLoops;

    size_t num_polygons = 0;
    size_t num_rings = 0;

    for (const auto &item : area) {
        if (item.type() == osmium::item_type::outer_ring) {
            if (num_polygons > 0) {
                shapes.push_back(toShape(env, outerLoop, innerLoops));
                innerLoops.clear();
            }
            outerLoop = toPointArray(env, static_cast<const osmium::OuterRing &>(item));
            num_rings++;
            num_polygons++;
        } else if (item.type() == osmium::item_type::inner_ring) {
            innerLoops.push_back(toPointArray(env, static_cast<const osmium::InnerRing &>(item)));
            num_rings++;
        }
    }

    // if there are no rings, this area is invalid
    if (num_rings == 0) {
        throw Exception{"area contains no rings?!?"};
    }

    shapes.push_back(toShape(env, outerLoop, innerLoops));

    jobjectArray shapesArray = env->NewObjectArray(shapes.size(), c_shape, nullptr);
    for (int i = 0; i < shapes.size(); i++) {
        env->SetObjectArrayElement(shapesArray, i, shapes[i]);
    }

    return popLocalFrame(env, env->NewObject(c_area, ctor_area, shapesArray));
}

extern "C" {

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_PolygonAssembler_init
        (JNIEnv *env, jclass cla) {
    c_area = (jclass) env->NewGlobalRef(env->FindClass("net/daporkchop/tpposmtilegen/geometry/Area"));
    c_shape = (jclass) env->NewGlobalRef(env->FindClass("net/daporkchop/tpposmtilegen/geometry/Shape"));
    c_point = (jclass) env->NewGlobalRef(env->FindClass("net/daporkchop/tpposmtilegen/geometry/Point"));
    c_point_array = (jclass) env->NewGlobalRef(env->FindClass("[Lnet/daporkchop/tpposmtilegen/geometry/Point;"));

    ctor_area = env->GetMethodID(c_area, "<init>", "([Lnet/daporkchop/tpposmtilegen/geometry/Shape;)V");
    ctor_shape = env->GetMethodID(c_shape, "<init>", "([Lnet/daporkchop/tpposmtilegen/geometry/Point;[[Lnet/daporkchop/tpposmtilegen/geometry/Point;)V");
    ctor_point = env->GetMethodID(c_point, "<init>", "(II)V");
}

JNIEXPORT jobject JNICALL Java_net_daporkchop_tpposmtilegen_natives_PolygonAssembler_assembleWay
        (JNIEnv *env, jclass cla, jlong wayId, jlong coordsAddr, jint coordsCount) {
    try {
        osmium::memory::Buffer wayBuffer(1024);
        auto nodes = (osmium::NodeRef *) coordsAddr;
        osmium::builder::add_way(wayBuffer, _id(wayId), _nodes(nodes, &nodes[coordsCount]));

        osmium::area::Assembler assembler(assembler_config);
        osmium::memory::Buffer areaBuffer(1024);
        if (!assembler(wayBuffer.get<osmium::Way>(0), areaBuffer)) {
            throw Exception{"assembler returned false?!?"};
        }

        return toArea(env, areaBuffer.get<osmium::Area>(0));
    } catch (const std::exception &e) {
        std::cerr << "while assembling area for way " << wayId << ": " << e.what() << std::endl;
        return nullptr;
    }
}

JNIEXPORT jobject JNICALL Java_net_daporkchop_tpposmtilegen_natives_PolygonAssembler_assembleRelation
        (JNIEnv *env, jclass cla, jlong relationId, jlongArray wayIds_, jlongArray coordAddrs_, jintArray coordCounts_, jbyteArray roles_) {
    try {
        int count = env->GetArrayLength(wayIds_);

        jlong *wayIds = env->GetLongArrayElements(wayIds_, nullptr);
        jlong *coordAddrs = env->GetLongArrayElements(coordAddrs_, nullptr);
        jint *coordCounts = env->GetIntArrayElements(coordCounts_, nullptr);
        jbyte *roles = env->GetByteArrayElements(roles_, nullptr);

        osmium::memory::Buffer wayBuffer(1024);
        std::vector<size_t> wayOffsets;
        wayOffsets.reserve(count);
        for (int i = 0; i < count; i++) {
            auto nodes = (osmium::NodeRef *) coordAddrs[i];
            wayOffsets.push_back(osmium::builder::add_way(wayBuffer, _id(wayIds[i]), _nodes(nodes, &nodes[coordCounts[i]])));
        }

        //get way references AFTER adding all of them to wayBuffer, because if the buffer is resized the pointers will be invalidated
        std::vector<const osmium::Way *> ways;
        ways.reserve(count);
        for (int i = 0; i < count; i++) {
            ways.push_back(&wayBuffer.get<osmium::Way>(wayOffsets[i]));
        }

        std::vector<member_type> relationMembers;
        relationMembers.reserve(count);
        for (int i = 0; i < count; i++) {
            static const char* ROLE_STRINGS_BY_ID[] = {
                    "outer",
                    "inner",
                    "",
                    "unknown"
            };
            relationMembers.emplace_back(member_type{osmium::item_type::way, wayIds[i], ROLE_STRINGS_BY_ID[roles[i]]});
        }

        osmium::memory::Buffer relationBuffer(1024);
        osmium::builder::add_relation(relationBuffer, _id(relationId), _members(relationMembers));
        const osmium::Relation& relation = relationBuffer.get<osmium::Relation>(0);

        env->ReleaseByteArrayElements(roles_, roles, 0);
        env->ReleaseIntArrayElements(coordCounts_, coordCounts, 0);
        env->ReleaseLongArrayElements(coordAddrs_, coordAddrs, 0);
        env->ReleaseLongArrayElements(wayIds_, wayIds, 0);

        osmium::area::Assembler assembler(assembler_config);
        osmium::memory::Buffer areaBuffer(1024);
        if (!assembler(relation, ways, areaBuffer)) {
            throw Exception{"assembler returned false?!?"};
        }

        return toArea(env, areaBuffer.get<osmium::Area>(0));
    } catch (const std::exception &e) {
        std::cerr << "while assembling area for relation " << relationId << ": " << e.what() << std::endl;
        return nullptr;
    }
}

}
