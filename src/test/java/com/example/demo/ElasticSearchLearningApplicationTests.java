package com.example.demo;

import com.alibaba.fastjson.JSON;
import com.example.demo.bean.User;
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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest
class ElasticSearchLearningApplicationTests {

	@Autowired
	@Qualifier("restHighLevelClient")
	private RestHighLevelClient client;

	//测试创建索引库
	@Test
	void createIndex() throws IOException {
		//创建请求
		CreateIndexRequest request = new CreateIndexRequest("test1");
		//执行请求
		CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
		System.out.println(response.toString());//org.elasticsearch.client.indices.CreateIndexResponse@6a5dc7e
	}


	//查看索引库是否存在 true存在，false不存在
	@Test
	void existsIndex() throws IOException {
		//创建请求
		GetIndexRequest request = new GetIndexRequest("test1");
		boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
		System.out.println(exists);
	}


	//删除索引库
	@Test
	void deleteIndex() throws IOException {
		DeleteIndexRequest request = new DeleteIndexRequest("test1");
		AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
		System.out.println(delete.isAcknowledged());//删除成功返回true，失败返回false
	}



	//文档操作================================================================================


	//添加文档
	@Test
	void createDocument() throws IOException {
		//创建添加数据
		User user = new User("张三",23);
		//声明要保存到那个索引库
		IndexRequest request = new IndexRequest("test1");
		request.id("1").timeout("1s");

		//给请求放入数据
		request.source(JSON.toJSONString(user), XContentType.JSON);
		//执行请求
		IndexResponse resp = client.index(request, RequestOptions.DEFAULT);
		System.out.println(resp);//和我们使用命令添加时显示的差不多
		System.out.println(resp.status());//CREATED
	}

	//修改文档
	@Test
	void updateDocument() throws IOException {
		//声明修改数据
		//User user = new User("李四",20);
		User user = new User();
		user.setName("王五");
		//声明索引库
		UpdateRequest request = new UpdateRequest("test1","1");
		request.id("1").timeout("1s");//设置修改的文档id和请求超时时间
		request.doc(JSON.toJSONString(user),XContentType.JSON);

		//执行修改  修改的时候，如果对象中某个字段没有给值，那么也会修改成默认值
		UpdateResponse update = client.update(request,RequestOptions.DEFAULT);
		System.out.println(update);
		System.out.println(update.status());//ok
	}

	//查看文档是否存在
	@Test
	void existsDocument() throws IOException {
		GetRequest request = new GetRequest("test1","1");
		boolean exists = client.exists(request, RequestOptions.DEFAULT);
		System.out.println(exists);//存在返回true，不存在返回false
	}

	//删除文档
	@Test
	void deleteDocument() throws IOException {
		DeleteRequest request = new DeleteRequest("test1","1");
		DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
		System.out.println(delete);
		System.out.println(delete.status());//ok
	}



	//根据id获取文档
	@Test
	void getDocument() throws IOException {
		GetRequest request = new GetRequest("test1","1");
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
		list.add(new User("chen1",20));
		list.add(new User("chen2",20));
		list.add(new User("chen3",20));
		list.add(new User("chen4",20));
		list.add(new User("chen5",20));
		list.add(new User("chen6",20));
		list.add(new User("chen7",20));
		list.add(new User("chen8",20));


		//注意：id要是重复，则会覆盖掉
		for (int i = 0; i < list.size(); i++) {
			bulkRequest.add(new IndexRequest("test1")
					.id(""+(i+1))
					.source(JSON.toJSONString(list.get(i)), XContentType.JSON));
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
		SearchRequest request = new SearchRequest("test1");
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

}


