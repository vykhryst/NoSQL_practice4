package org.nosql.vykhryst.dao.entityDao;


import org.nosql.vykhryst.dao.DAO;
import org.nosql.vykhryst.entity.Advertising;
import org.nosql.vykhryst.entity.Program;

import java.util.Map;


public interface ProgramDAO extends DAO<Program> {

    boolean saveAdvertisingToProgram(String programId, Map<Advertising, Integer> advertising);
    boolean deleteAdvertisingFromProgram(String programId, String advertisingId);

    String migrate(Program program);
}
