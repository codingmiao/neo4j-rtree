package org.wowtools.neo4j.rtree.util;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * 自定义的函数、存储过程注册工具
 * @author liuyu
 * @date 2022/5/25
 */
public class CustomProceduresRegister {

    /**
     * 将一个类中的存储过程和函数注册到db
     *
     * @param procedureClass 含有存储过程或函数注解的类
     */
    public static void registerProcedures(GraphDatabaseService graphDb, Class<?> procedureClass) {
        GlobalProcedures globalProcedures = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency(GlobalProcedures.class);
        try {
            globalProcedures.registerProcedure(procedureClass);
            globalProcedures.registerFunction(procedureClass);
            globalProcedures.registerAggregationFunction(procedureClass);
        } catch (KernelException e) {
            throw new RuntimeException("while registering " + procedureClass, e);
        }
    }

}
