package com.zlx.demo;

public class Student {

    private String name;

    private int age;

    private int num;

    private String email;

    // 提供一个静态builder方法
    public static Student.Builder builder() {
        return new Student.Builder();
    }
    // 外部调用builder类的属性接口进行设值。
    public static class Builder{
        private String name;

        private int age;

        private int num;

        private String email;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder age(int age) {
            this.age = age;
            return this;
        }

        public Builder num(int num) {
            this.num = num;
            return this;
        }

        public Builder email(String email) {
            this.email = email;
            return this;
        }

        public Student build() {
            // 将builder对象传入到学生构造函数
            return new Student(this);
        }
    }
    // 私有化构造器
    private Student(Builder builder) {
        name = builder.name;
        age = builder.age;
        num = builder.num;
        email = builder.email;
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", num=" + num +
                ", email='" + email + '\'' +
                '}';
    }

    public static void main(String[] args) {
        Student.builder()
                .name("平头哥")
                .num(1)
                .age(18)
                .email("平头哥@163.com")
                .build();

    }
}