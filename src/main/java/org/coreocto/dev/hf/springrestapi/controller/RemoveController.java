package org.coreocto.dev.hf.springrestapi.controller;

import org.apache.log4j.Logger;
import org.coreocto.dev.hf.springrestapi.AppConstants;
import org.coreocto.dev.hf.springrestapi.bean.BaseResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@RestController
public class RemoveController {

    private Logger LOGGER = Logger.getLogger(this.getClass());

    @Autowired
    JdbcTemplate jdbcTemplate;

    @RequestMapping(path = "/remove")
    public BaseResponse remove(
            @RequestParam("docId") String docId
    ) {
        BaseResponse response = new BaseResponse();
        response.setMessage(AppConstants.MSG_OK);
        response.setStatus(AppConstants.STATUS_OK);

        int rowCnt = -1;

        rowCnt = jdbcTemplate.update("update tdocuments set cdelete = ? where cdocid = ?", new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
                ps.setInt(1, 1);
                ps.setString(2, docId);
            }
        });

        if (rowCnt != 1) {
            String msg = "error when inserting tstatistics record";
            LOGGER.error(msg);
            response.setMessage(msg);
            response.setStatus(AppConstants.STATUS_ERR);
        }

        return response;
    }
}
