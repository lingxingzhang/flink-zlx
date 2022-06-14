package com.zlx.base;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;
import java.util.logging.Handler;


/**
 * 各类 transformation算子 map flatmap filter proj
 */
public class _07_Transformation_demos {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("data/transformation_input/transformation");

        SingleOutputStreamOperator<UserInfo> map = source.map(json -> JSON.parseObject(json,UserInfo.class));
        //map.print("类型转换");

        /**
         * 需求：提取每个人的朋友信息 和 主信息就行组合 转发
         */
        map.flatMap(new FlatMapFunction<UserInfo, UserInfoDetail>() {
            @Override
            public void flatMap(UserInfo value, Collector<UserInfoDetail> out) throws Exception {
                List<FriendsInfo> friendsInfoList = value.getFriends();
                for (FriendsInfo friendsInfo : friendsInfoList) {
                    out.collect(new UserInfoDetail(value.getUid(),value.getName(),
                            value.getGender(),friendsInfo.getFid(),friendsInfo.getName()));
                }
            }
        })/*.print("将朋友压平展示：")*/;

        /**
         * keyBy 算子的展示
         * 滚动聚合算子 只能作用在keyedStream上 比如：sum算子，min算子，max算子 minBy maxBy 算子 reduce算子
         *
         * 需求：
         *  统计各性别的好友总数
         *  统计各性别的的最大朋友数
         *  统计各性别的的最小朋友数
         */

        //  3.1 统计各性别的好友总数
        // 先转换为Tuple4的结果 将好友的listsize进行计算并形成一个字段 uid name gender fcount【好友数量】

        map
                .map(bean -> Tuple2.of(bean.getGender(), bean.getFriends().size()))
                .returns(new TypeHint<Tuple2<String, Integer>>() {})
                .keyBy(tuple2 -> tuple2.f0)
                .sum("f1")
                /*.print("统计各性别的好友总数: ")*/;


        // 3.2 统计各性别的的最大朋友数 主要联系max maxBy算子的区别
        map
                .map(bean -> Tuple4.of(bean.getUid(), bean.getName(), bean.getGender(), bean.getFriends().size()))
                .returns(new TypeHint<Tuple4<String, String, String, Integer>>() {
                })
        .keyBy(t -> t.f2)
        .maxBy(3)
        /*.print("统计各性别的的最大朋友数:")*/
        ;

        /**
         * 结果分析：我们分析一下分组为 male的数据
         *  原始数据格式
         *  {"uid":1,"gender":"male","name":"zs","friends":[{"fid":2,"name":"aa"}]}
         * {"uid":2,"gender":"male","name":"ls","friends":[{"fid":1,"name":"cc"},{"fid":2,"name":"aa"}]}
         * {"uid":5,"gender":"male","name":"tq","friends":[{"fid":2,"name":"aa"},{"fid":3,"name":"bb"}]}
         *
         * 结果：
         *  统计各性别的的最大朋友数:> (1,zs,male,1)
         * 统计各性别的的最大朋友数:> (1,zs,male,2)
         * 统计各性别的的最大朋友数:> (1,zs,male,2)
         *
         * 我们的分组条件是gender，聚合条件是friendsList写成sql的为
         *  select uid,name,gender,max(friendsListSize) from tableA group by gender
         *      max的策略是：非分组字段的值，取第一条数据的值： uid,name 所以最终结果为(1,zs,male,2)
         *      maxBy的策略是：替换为最新的结果（大于的时候才会替换）uid,name 所以最终结果为 (2,ls,male,2)
         *          因为第三条数据和第二条数据的friendsListSize都是等于2的，所以没有进行替换
         */

        // 自己实现替换逻辑 等于的时候 也进行替换
        map
                .map(bean -> Tuple4.of(bean.getUid(), bean.getName(), bean.getGender(), bean.getFriends().size()))
                .returns(new TypeHint<Tuple4<String, String, String, Integer>>() {
                })
                .keyBy(t -> t.f2)
                .reduce(new ReduceFunction<Tuple4<String, String, String, Integer>>() {
                    /**
                     * @param value1 历史之前的值
                     * @param value2 最新来的数据
                     * @return
                     * @throws Exception
                     */
                    @Override
                    public Tuple4<String, String, String, Integer> reduce(Tuple4<String, String, String, Integer> value1,
                                                                          Tuple4<String, String, String, Integer> value2) throws Exception {
                        if (value2.f3 >= value1.f3){
                            return value2;
                        }else {
                            return value1;
                        }
                    }
                })
               /* .print("统计各性别的的最大朋友数[使用reduce]:")*/
                ;


        map
                .map(bean -> Tuple4.of(bean.getUid(), bean.getName(), bean.getGender(), bean.getFriends().size()))
                .returns(new TypeHint<Tuple4<String, String, String, Integer>>() {
                })
                .keyBy(t -> t.f2)
                .reduce(new ReduceFunction<Tuple4<String, String, String, Integer>>() {
                    /**
                     * @param value1 历史之前的值
                     * @param value2 最新来的数据
                     * @return
                     * @throws Exception
                     */
                    @Override
                    public Tuple4<String, String, String, Integer> reduce(Tuple4<String, String, String, Integer> value1,
                                                                          Tuple4<String, String, String, Integer> value2) throws Exception {
                        value2.setField(value2.f3+(value1 == null? 0:value1.f3),3);
                        return value2;
                    }
                })
                .print("使用reduce实现sum聚合:")
        ;

        env.execute("_07_Transformation_demos");


    }

}

@Data
class UserInfo implements Serializable {
    private String uid;
    private String name;
    private String gender;
    private List<FriendsInfo> friends;
}

@Data
class FriendsInfo implements Serializable{
    private String fid;
    private String name;
}

/**
 * 用来展示flatmap压平的结果
 */
@Data
@AllArgsConstructor
class UserInfoDetail implements Serializable {
    private String uid;
    private String name;
    private String gender;
    private String fid;
    private String fname;
}