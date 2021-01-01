#include "tpposmtilegen_common.h"

class Exception : public std::exception {
private:
    std::string m_text;
public:
    Exception(std::string text) : m_text(std::move(text)) {};
    Exception(std::string& text) : m_text(text) {};

    const char *what() const noexcept override {
        return m_text.c_str();
    }
};

#include <osmium/builder/attr.hpp>
using namespace osmium::builder::attr;

static jclass c_area;
static jclass c_shape;
static jclass c_shape_array;
static jclass c_point;

static jmethodID ctor_area;
static jmethodID ctor_shape;
static jmethodID ctor_point;

jobject toShape(JNIEnv* env, jobjectArray outerLoop, std::vector<jobjectArray>& innerLoops) {
    jobjectArray innerLoopsArray = env->NewObjectArray(innerLoops.size(), c_shape_array, nullptr);
    for (int i = 0; i < innerLoops.size(); i++) {
        env->SetObjectArrayElement(innerLoopsArray, i, innerLoops[i]);
    }

    return env->NewObject(c_shape, ctor_shape, outerLoop, innerLoopsArray);
}

jobjectArray toPointArray(JNIEnv* env, const osmium::NodeRefList& nodes) {
    jobjectArray array = env->NewObjectArray(nodes.size(), c_point, nullptr);
    for (int i = 0; i < nodes.size(); i++) {
        jobject point = env->NewObject(c_point, ctor_point, nodes[i].x(), nodes[i].y());
        env->SetObjectArrayElement(array, i, point);
    }
    return array;
}

jobject toArea(JNIEnv* env, jlong id, const osmium::Area& area) {
    std::vector<jobject> shapes;

    jobjectArray outerLoop;
    std::vector<jobjectArray> innerLoops;

    size_t num_polygons = 0;
    size_t num_rings = 0;

    for (const auto& item : area) {
        if (item.type() == osmium::item_type::outer_ring) {
            if (num_polygons > 0) {
                shapes.push_back(toShape(env, outerLoop, innerLoops));
                innerLoops.clear();
            }
            outerLoop = toPointArray(env, static_cast<const osmium::OuterRing&>(item));
            ++num_rings;
            ++num_polygons;
        } else if (item.type() == osmium::item_type::inner_ring) {
            innerLoops.push_back(toPointArray(env, static_cast<const osmium::InnerRing&>(item)));
            ++num_rings;
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

    return env->NewObject(c_area, ctor_area, id, shapesArray);
}

extern "C" {

JNIEXPORT void JNICALL Java_net_daporkchop_tpposmtilegen_natives_PolygonAssembler_init
        (JNIEnv *env, jclass cla) {
    c_area = (jclass) env->NewGlobalRef(env->FindClass("net/daporkchop/tpposmtilegen/osm/area/Area"));
    c_shape = (jclass) env->NewGlobalRef(env->FindClass("net/daporkchop/tpposmtilegen/osm/area/Shape"));
    c_shape_array = (jclass) env->NewGlobalRef(env->FindClass("[Lnet/daporkchop/tpposmtilegen/osm/area/Shape;"));
    c_point = (jclass) env->NewGlobalRef(env->FindClass("net/daporkchop/tpposmtilegen/util/Point"));

    ctor_area = env->GetMethodID(c_area, "<init>", "(J[Lnet/daporkchop/tpposmtilegen/osm/area/Shape;)V");
    ctor_shape = env->GetMethodID(c_shape, "<init>", "([Lnet/daporkchop/tpposmtilegen/util/Point;[[Lnet/daporkchop/tpposmtilegen/util/Point;)V");
    ctor_point = env->GetMethodID(c_point, "<init>", "(II)V");
}

JNIEXPORT jobject JNICALL Java_net_daporkchop_tpposmtilegen_natives_PolygonAssembler_assembleWay
        (JNIEnv *env, jclass cla, jlong id, jlong wayId, jlong coordsAddr, jint coordsCount) {
    try {
        osmium::area::Assembler::config_type assembler_config;
        osmium::area::Assembler assembler(assembler_config);

        osmium::memory::Buffer wayBuffer(1024);
        auto nodes = (osmium::NodeRef*) coordsAddr;
        osmium::builder::add_way(wayBuffer, _id(wayId), _nodes(nodes, &nodes[coordsCount]));

        osmium::memory::Buffer areaBuffer(1024);
        if (!assembler(wayBuffer.get<osmium::Way>(0), areaBuffer)) {
            throw Exception{"assembler returned false?!?"};
        }

        return toArea(env, id, areaBuffer.get<osmium::Area>(0));
    } catch (const std::exception &e) {
        std::cout << "while assembling area for way " << wayId << ": " << e.what() << std::endl;
        return nullptr;
    }
}

}
