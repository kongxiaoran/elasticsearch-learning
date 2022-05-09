package com.example.demo;

import com.alibaba.fastjson.JSON;
import com.example.demo.bean.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 该类 大部分 copy from https://blog.csdn.net/weixin_43431123/article/details/114603136
 */
@SpringBootTest
class ElasticSearchLearningApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    //测试创建索引库
    @Test
    void createIndex() throws IOException {
        //创建请求
        CreateIndexRequest request = new CreateIndexRequest("users");
        //执行请求
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println("创建索引 " + ((response.isAcknowledged() == true) ? "成功了" : "失败了"));        // 响应状态:true or false
    }


    //查看索引库是否存在 true存在，false不存在.如果存在则，打印索引库相关信息
    @Test
    void existsIndex() throws IOException {
        //创建请求
        GetIndexRequest request = new GetIndexRequest("users");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println("索引" + (exists == true ? "" : "不") + "存在");
        if (exists) {
            GetIndexResponse getIndexResponse =
                    client.indices().get(request, RequestOptions.DEFAULT);
            System.out.println(getIndexResponse.getAliases());
            System.out.println(getIndexResponse.getMappings());
            System.out.println(getIndexResponse.getSettings());
        }
    }


    //删除索引库
    @Test
    void deleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("users");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());//删除成功返回true，失败返回false
    }


    //文档操作================================================================================


    //添加文档-1
    @Test
    void createDocument() throws IOException {
        //创建添加数据
        User user = new User("张三", 23, "男");
        //声明要保存到那个索引库
        IndexRequest request = new IndexRequest("users");
        request.id("001").timeout("1s");

        //给请求放入数据.向ES中插入数据，必须将数据转换成 JSON 格式
        request.source(JSON.toJSONString(user), XContentType.JSON);
        //执行请求
        IndexResponse resp = client.index(request, RequestOptions.DEFAULT);
        System.out.println(resp);//和我们使用命令添加时显示的差不多
        System.out.println(resp.status());//CREATED
    }

    //添加文档-2
    @Test
    void createDocument2() throws IOException {
        //创建添加数据
        User user = new User("莉丝", 23, "女");
        //声明要保存到那个索引库
        IndexRequest request = new IndexRequest("users");
        request.id("002").timeout("1s");

        //给请求放入数据.向ES中插入数据，必须将数据转换成 JSON 格式
        request.source(new ObjectMapper().writeValueAsString(user), XContentType.JSON);
        //执行请求
        IndexResponse resp = client.index(request, RequestOptions.DEFAULT);
        System.out.println(resp);//和我们使用命令添加时显示的差不多
        System.out.println(resp.status());//CREATED
    }

    //修改文档
    @Test
    void updateDocument() throws IOException {
        //声明修改数据
        User user = new User();
        user.setAge(32);
        //声明索引库
        UpdateRequest request = new UpdateRequest("users", "001");
        request.id("001").timeout("1s");//设置修改的文档id和请求超时时间
        request.doc(JSON.toJSONString(user), XContentType.JSON);
//		request.doc(XContentType.JSON,"sex","男");

        //执行修改  修改的时候，如果对象中某个字段没有给值，那么也会修改成默认值
        UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        System.out.println(update);
        System.out.println(update.status());
    }

    //查看文档是否存在
    @Test
    void existsDocument() throws IOException {
        GetRequest request = new GetRequest("users", "1");
        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);//存在返回true，不存在返回false
    }

    //删除文档
    @Test
    void deleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("users", "001");
        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete);
        System.out.println(delete.status());
    }


    //根据id获取文档
    @Test
    void getDocument() throws IOException {
        GetRequest request = new GetRequest("users", "2");
        GetResponse resp = client.get(request, RequestOptions.DEFAULT);
        System.out.println(resp);
        System.out.println(resp.getSourceAsString());//获取文档内容的字符串，没有数据为null
    }


    //批量操作,修改和删除操作只是改变request即可
    @Test
    void bulkadd() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        List<User> list = new ArrayList<>();
        list.add(new User("chen1", 20, "男"));
        list.add(new User("chen2", 21, "男"));
        list.add(new User("chen3", 22, "男"));
        list.add(new User("chen4", 23, "男"));
        list.add(new User("chen5", 24, "男"));
        list.add(new User("chen6", 25, "男"));
        list.add(new User("chen7", 26, "男"));
        list.add(new User("chen8", 27, "男"));


        //注意：id要是重复，则会覆盖掉
        for (int i = 0; i < list.size(); i++) {
            bulkRequest.add(new IndexRequest("users")
                    .id("" + (i + 1))
                    .source(JSON.toJSONString(list.get(i)), XContentType.JSON));
        }
        //执行
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk);
        System.out.println(bulk.status());
    }

    //批量删除文档
    @Test
    void bulkdelete() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        for (int i = 0; i < 8; i++) {
            bulkRequest.add(new DeleteRequest("users", "" + (i + 1)));
        }
        //执行
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk);
        System.out.println(bulk.status());

    }

    //条件查询文档
    @Test
    void searchDocument() throws IOException {
        //声明请求
        SearchRequest request = new SearchRequest("users");
        //创建查询构造器对象
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //精准查询条件构造器，还可以封装很多的构造器,都可以使用QueryBuilders这个类构建
        //QueryBuilders里面封装了我们使用的所有查询筛选命令
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "chen1");
        //把查询条件构造器放入到查询构造器中
        builder.query(termQueryBuilder);

        //把条件构造器放入到请求中
        request.source(builder);
        //执行查询
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        //这个查询就和我们使用命令返回的结果是一致的
        System.out.println(JSON.toJSONString(search.getHits().getHits()));
        System.out.println("==============================================");
        for (SearchHit hit : search.getHits().getHits()) {
            //遍历获取到的hits，让每一个hit封装为map形式
            System.out.println(hit.getSourceAsMap());
        }
    }

    //分页 查询文档
    @Test
    void searchDocumentPage() throws IOException {
        //声明请求
        SearchRequest request = new SearchRequest("users");
        //创建查询构造器对象
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "chen1");

        //把条件构造器放入到请求中。这里初始查询构造器时就设置了分页的size与from
        request.source(new SearchSourceBuilder().from(0).size(100).query(termQueryBuilder));

        //执行查询
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        //这个查询就和我们使用命令返回的结果是一致的
        System.out.println(JSON.toJSONString(search.getHits().getHits()));
        System.out.println("==============================================");
        for (SearchHit hit : search.getHits().getHits()) {
            //遍历获取到的hits，让每一个hit封装为map形式
            System.out.println(hit.getSourceAsMap());
        }
    }

    //查询文档-按指定字段排序
    @Test
    void searchDocumentSort() throws IOException {
        //声明请求
        SearchRequest request = new SearchRequest("users");
        //创建查询构造器对象
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "chen1");

        //把条件构造器放入到请求中。这里初始查询构造器时就设置了分页的size与from以及排序规则
        request.source(new SearchSourceBuilder().from(0).size(100).sort("age", SortOrder.DESC).query(termQueryBuilder));

        //执行查询
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        //这个查询就和我们使用命令返回的结果是一致的
        System.out.println(JSON.toJSONString(search.getHits().getHits()));
        System.out.println("==============================================");
        for (SearchHit hit : search.getHits().getHits()) {
            //遍历获取到的hits，让每一个hit封装为map形式
            System.out.println(hit.getSourceAsMap());
        }
    }

    //查询文档-过滤一些不需要的字段,只显示需要的字段
    @Test
    void searchDocumentFilter() throws IOException {
        //声明请求
        SearchRequest request = new SearchRequest("users");
        //创建查询构造器对象
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "chen1");

        //把条件构造器放入到请求中。这里初始查询构造器时就设置了分页的size与from以及排序规则
        request.source(new SearchSourceBuilder().from(0).size(100).sort("age", SortOrder.DESC)
                .fetchSource(new String[]{"name"}, new String[]{}).query(termQueryBuilder));

        //执行查询
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        //这个查询就和我们使用命令返回的结果是一致的
        System.out.println(JSON.toJSONString(search.getHits().getHits()));
        System.out.println("==============================================");
        for (SearchHit hit : search.getHits().getHits()) {
            //遍历获取到的hits，让每一个hit封装为map形式
            System.out.println(hit.getSourceAsMap());
        }
    }

    //组合查询
    @Test
    void searchDocumentCombination() throws IOException {
        //声明请求
        SearchRequest request = new SearchRequest("users");

        //初始化条件构造器：设置组合查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 必须 age = 24 && sex!="男"
//		boolQueryBuilder.must(QueryBuilders.matchQuery("age",24));
//		boolQueryBuilder.mustNot(QueryBuilders.matchQuery("sex","男"));

        // 必须 age = 24 || sex = "男"
        boolQueryBuilder.should(QueryBuilders.matchQuery("age", 24));
        boolQueryBuilder.should(QueryBuilders.matchQuery("sex", "男"));

        //把条件构造器放入到请求中。这里初始查询构造器时就设置了分页的size与from以及排序规则，然后查询构造器再执行
        request.source(new SearchSourceBuilder().from(0).size(100).sort("age", SortOrder.DESC)
                .fetchSource(new String[]{"name"}, new String[]{}).query(boolQueryBuilder));

        //执行查询
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        //这个查询就和我们使用命令返回的结果是一致的
        System.out.println(JSON.toJSONString(search.getHits().getHits()));
        System.out.println("==============================================");
        for (SearchHit hit : search.getHits().getHits()) {
            //遍历获取到的hits，让每一个hit封装为map形式
            System.out.println(hit.getSourceAsMap());
        }
    }

    //范围查询
    @Test
    void searchDocumentRange() throws IOException {

        SearchRequest request = new SearchRequest();
        request.indices("users");

        SearchSourceBuilder builder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age");
        rangeQueryBuilder.gte(25).lt(30);            // 25<= x <30
        builder.query(rangeQueryBuilder);
        request.source(builder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //范围查询 & 模糊查询
    @Test
    void searchVagueDocument() throws IOException {

        SearchRequest request = new SearchRequest().indices("users");

        SearchSourceBuilder builder = new SearchSourceBuilder();
        // name 字段进行模糊查询 寻找 符合 chen ，可容忍误差在 1个字符内
        FuzzyQueryBuilder fuzzinessBuild = QueryBuilders.fuzzyQuery("name", "chen").fuzziness(Fuzziness.ONE);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age").gte(22).lt(30);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(fuzzinessBuild).must(rangeQueryBuilder);
        builder.query(boolQueryBuilder);

        request.source(builder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //高亮查询-暂未实现成功
    @Test
    void searchDocumentHighlight() throws IOException {

        SearchSourceBuilder builder = new SearchSourceBuilder();
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "chen1");
        //将 termQuery 查询的内容 高亮显示. 高亮的格式可以自定义
//        builder.highlighter(new HighlightBuilder().preTags("<font color='red'>").postTags("</font>").field("name"))
//                .query(termQueryBuilder);
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field("name");
        builder.highlighter(highlightBuilder);
        builder.query(termQueryBuilder);

        SearchResponse response = client.search(new SearchRequest().indices("users").source(builder)
                ,RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println(JSON.toJSONString(response.getHits()));
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    // 最大值查询
    @Test
    void searchDocumentPolymerization() throws IOException {

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.aggregation(AggregationBuilders.max("maxAge").field("age"));

        SearchResponse response = client.search(new SearchRequest().indices("users").source(builder)
                ,RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        //查询到的最大值的情况
        System.out.println(JSON.toJSONString(response.getAggregations()));
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    // 分组查询
    @Test
    void searchDocumentGroup() throws IOException {

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.aggregation(AggregationBuilders.terms("age").field("age"));

        SearchResponse response = client.search(new SearchRequest().indices("users").source(builder)
                ,RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        //查询到的最大值的情况
        System.out.println(JSON.toJSONString(response.getAggregations()));
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }


}


