package org.hhoao.test.flink.hive;

public class User {
    private Integer id;
    private String name;
    private Double money;
    private int age;

    public User(Integer id, String name, Double money, int age) {
        this.id = id;
        this.name = name;
        this.money = money;
        this.age = age;
    }

    public User() {}

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "User{"
                + "id="
                + id
                + ", name='"
                + name
                + '\''
                + ", money="
                + money
                + ", age="
                + age
                + '}';
    }
}
