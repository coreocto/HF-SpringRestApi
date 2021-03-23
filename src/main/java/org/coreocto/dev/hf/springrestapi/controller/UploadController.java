package org.coreocto.dev.hf.springrestapi.controller;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.coreocto.dev.hf.commonlib.Constants;
import org.coreocto.dev.hf.commonlib.sse.chlh.Index;
import org.coreocto.dev.hf.commonlib.sse.suise.bean.AddTokenResult;
import org.coreocto.dev.hf.commonlib.sse.vasst.bean.TermFreq;
import org.coreocto.dev.hf.springrestapi.AppConstants;
import org.coreocto.dev.hf.springrestapi.bean.BaseResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
public class UploadController {

    private Logger LOGGER = Logger.getLogger(this.getClass());

    @Autowired
    JdbcTemplate jdbcTemplate;

    @RequestMapping(path = "/upload")//, method = RequestMethod.POST
    public BaseResponse upload
            (
                    @RequestParam("docId") String docId,
                    @RequestParam(value = "token", required = false) String tokenInJson,
                    @RequestParam("ft") String ft,
                    @RequestParam("st") String st,
                    @RequestParam("weiv") String weiv,
                    @RequestParam("feiv") String feiv,
                    @RequestParam(value = "terms", required = false) String terms,
                    @RequestParam(value = "index", required = false) String index_in_json

            ) {
        BaseResponse response = new BaseResponse();
        response.setMessage("ok");
        response.setStatus(AppConstants.STATUS_OK);

//        if (st == null || st.isEmpty()) {
//            st = Constants.SSE_TYPE_SUISE + "";
//        }

        if (docId == null || ((st.equals(Constants.SSE_TYPE_SUISE + "") ||
                st.equals(AppConstants.SSE_TYPE_SUISE_2 + "") ||
                st.equals(AppConstants.SSE_TYPE_SUISE_3 + "")) && tokenInJson == null)) {
            return response;
        }

        int rowCnt = 0;

        // we need to insert a document record here

        if (response.getStatus() < AppConstants.STATUS_ERR) {
            rowCnt = jdbcTemplate.update("insert into tdocuments (cdocid,cdelete,cft,cst,cweiv,cfeiv) select ? as text, ? as integer, cast(? as integer), cast(? as integer), ? as text, ? as text where not exists (select 1 from tdocuments where cdocid = ? and cdelete = ?)", new PreparedStatementSetter() {

                @Override
                public void setValues(PreparedStatement pStmnt) throws SQLException {
                    int paramIdx = 1;

                    pStmnt.setString(paramIdx++, docId);
                    pStmnt.setInt(paramIdx++, 0);
                    pStmnt.setString(paramIdx++, ft);
                    pStmnt.setString(paramIdx++, st);
                    pStmnt.setString(paramIdx++, weiv);
                    pStmnt.setString(paramIdx++, feiv);
                    pStmnt.setString(paramIdx++, docId);
                    pStmnt.setInt(paramIdx++, 0);
                }
            });

            if (rowCnt != 1) {
                String msg = "error when inserting tdocuments record";
                LOGGER.debug(msg);
                response.setStatus(AppConstants.STATUS_ERR);
                response.setMessage(msg);
            }
        }

        Gson gson = new Gson();

        if (response.getStatus() < AppConstants.STATUS_ERR) {

            if (st.equalsIgnoreCase(Constants.SSE_TYPE_SUISE + "")) {

                AddTokenResult addTokenResult = null;

                try {
                    addTokenResult = gson.fromJson(tokenInJson, AddTokenResult.class);
                } catch (Exception ex) {
                    String msg = "error when parsing json into object";
                    LOGGER.error(msg, ex);
                    response.setStatus(AppConstants.STATUS_ERR);
                    response.setMessage(msg);
                }

                if (response.getStatus() < AppConstants.STATUS_ERR) {
                    final String tmpId = addTokenResult.getId();
                    final List<String> tmpC = addTokenResult.getC();

                    int[] updRowCnt = jdbcTemplate.batchUpdate("insert into tdocument_indexes (cdocid,ctoken,corder) values (?,?,?)", new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            ps.setString(1, tmpId);
                            ps.setString(2, tmpC.get(i));
                            ps.setInt(3, i);
                        }

                        @Override
                        public int getBatchSize() {
                            return tmpC.size();
                        }
                    });
                }

            } else if (st.equalsIgnoreCase(Constants.SSE_TYPE_VASST + "")) {

                TermFreq termFreq = null;

                try {
                    termFreq = gson.fromJson(terms, TermFreq.class);
                } catch (Exception ex) {
                    String msg = "error when parsing json into object";
                    LOGGER.error(msg, ex);
                    response.setStatus(AppConstants.STATUS_ERR);
                    response.setMessage(msg);
                }

                if (response.getStatus() < AppConstants.STATUS_ERR) {
                    Map<String, Integer> termsMap = termFreq.getTerms();
                    List<String> keys = new ArrayList<>(termsMap.keySet());

                    int[] updRowCnt = jdbcTemplate.batchUpdate("insert into tdoc_term_freq (cdocid,cword,ccount) values (?,?,?)", new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            String key = keys.get(i);
                            ps.setString(1, docId);
                            ps.setString(2, key);
                            ps.setInt(3, termsMap.get(key));
                        }

                        @Override
                        public int getBatchSize() {
                            return keys.size();
                        }
                    });
                }

//                try (PreparedStatement pStmnt = con.prepareStatement("insert into tdoc_term_freq (cdocid,cword,ccount) values (?,?,?)")) {
//
//                    Map<String, Integer> termsMap = termFreq.getTerms();
//                    for (String key : termsMap.keySet()) {
//                        Integer value = termsMap.get(key);
//
//                        pStmnt.clearParameters();
//                        pStmnt.setString(1, docId);
//                        pStmnt.setString(2, key);
//                        pStmnt.setInt(3, value);
//                        rowCnt += pStmnt.executeUpdate();
//                    }
//
//                } catch (Exception e) {
//                    response.setStatus(STATUS_ERR);
//                    response.setMessage("error when inserting entry on tdoc_term_freq");
//                }


            } else if (st.equalsIgnoreCase(Constants.SSE_TYPE_CHLH + "")) {
                Index index = null;
                try {
                    index = gson.fromJson(index_in_json, Index.class);
                } catch (Exception ex) {
                    String msg = "error when parsing json into object";
                    LOGGER.error(msg, ex);
                    response.setStatus(AppConstants.STATUS_ERR);
                    response.setMessage(msg);
                }

                if (response.getStatus() < AppConstants.STATUS_ERR) {
                    List<String> bloomFilters = index.getBloomFilters();

                    String encDocId = index.getDocId();

                    int[] updRowCnt = jdbcTemplate.batchUpdate("insert into tchlh (cdocid, cbf) values (?,?)", new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            String s = bloomFilters.get(i);
                            ps.setString(1, encDocId);
                            ps.setString(2, s);
                        }

                        @Override
                        public int getBatchSize() {
                            return bloomFilters.size();
                        }
                    });
                }

//                try (PreparedStatement insertStmnt = con.prepareStatement("insert into tchlh (cdocid, cbf) values (?,?)")) {
//
//                    for (String s : index.getBloomFilters()) {
//                        insertStmnt.clearParameters();
//                        insertStmnt.setString(1, index.getDocId());
//                        insertStmnt.setString(2, s);
//                        insertStmnt.executeUpdate();
//                    }
//
//                } catch (Exception e) {
//                    String msg = "error when inserting record to tchlh";
//                    LOGGER.error(msg, e);
//                    response.setStatus(STATUS_ERR);
//                    response.setMessage(msg);
//                }
            }

        }

        return response;
    }
}
