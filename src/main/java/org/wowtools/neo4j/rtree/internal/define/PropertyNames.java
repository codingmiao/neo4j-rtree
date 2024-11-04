package org.wowtools.neo4j.rtree.internal.define;

/**
 * 属性名
 *
 * @author liuyu
 * @date 2021/12/18
 */
public class PropertyNames {

    /**
     * mbr的左下角
     */
    public static final String mbrMin = "mbrMin";

    /**
     * mbr的右上角
     */
    public static final String mbrMax = "mbrMax";

    /**
     * node类型
     */
    public static final String nodeType = "nodeType";


    /**
     * 叶子节点中的元素个数
     */
    public static final String size = "size";


    /**
     * 叶子节点中的元素个数
     */
    public static final String entryDataId = "entryDataId";
    /**
     * 叶子节点中的元素bbox前缀
     */
    public static final String entryMax = "entryMax";
    /**
     * 叶子节点中的元素bbox前缀
     */
    public static final String entryMin = "entryMin";
}
