import random
import json
import os
from faker import Faker
import numpy as np

fake = Faker()

a = 2
scale_fator = 2
# 定义数据生成的数量
num_users = 1000 * scale_fator
num_posts = 2000 * scale_fator
num_comments = 5000 * scale_fator

vertex_num = 0
edge_num = 0
hyperedge_num = 0

# 生成用户
def generate_users(num_users):
    global vertex_num
    users = []
    for _ in range(num_users):
        user = {
            "id": fake.uuid4(),
            "name": fake.name(),
            "birthdate": fake.date_of_birth().isoformat(),
            "email": fake.email()
        }
        users.append(user)
    vertex_num += num_users
    return users

# 生成关注关系
def generate_follows(users):
    global vertex_num, edge_num, hyperedge_num
    follows = []
    for user in users:
        num_follows = int(np.random.power(a) * (num_users - 1))
        hyperedge_num += 1
        edge_num += num_follows
        follow_users = []
        for _ in range(num_follows):
            choice = random.choice(users)["id"]
            while choice == user["id"]:
                choice = random.choice(users)["id"]
            follow_users.append(choice)
        follow = {
            "follower_id": user["id"],
            "followee_id": follow_users
        }
        follows.append(follow)
    return follows

# 生成帖子
def generate_posts(num_posts, user_ids):
    global vertex_num, edge_num, hyperedge_num
    posts = []
    indices = (np.random.power(a, num_posts) * (num_users - 1)).astype(int)
    # print(indices)
    for index in indices:
        post = {
            "id": fake.uuid4(),
            "creator_id": user_ids[index],
            "created_at": fake.date_time_this_year().isoformat(),
            "content": fake.text()
        }
        posts.append(post)
    vertex_num += num_posts
    edge_num += num_posts
    hyperedge_num += num_posts
    return posts

# 生成评论
def generate_comments(num_comments, user_ids, post_ids):
    global vertex_num, edge_num, hyperedge_num
    comments = []
    user_indices = (np.random.power(a, num_comments) * (num_users - 1)).astype(int)
    post_indices = (np.random.power(a, num_comments) * (num_posts - 1)).astype(int)
    for index in range(num_comments):
        comment = {
            "id": fake.uuid4(),
            "creator_id": user_ids[user_indices[index]],
            "post_id": post_ids[post_indices[index]],
            "created_at": fake.date_time_this_year().isoformat(),
            "content": fake.sentence()
        }
        comments.append(comment)
    vertex_num += num_comments
    edge_num += num_comments * 2
    hyperedge_num += num_comments
    return comments

def persist_data(users, posts, comments, follows, path):
    if not os.path.exists(path):
        os.mkdir(path)
        
    # 保存描述数据
    with open(os.path.join(path, 'description.json'), 'w') as f:
        description = {
            "vertex_num": vertex_num,
            "edge_num": edge_num,
            "hyperedge_num": hyperedge_num
        }
        json.dump(description, f)

    # 保存数据到JSON文件
    with open(os.path.join(path, 'users.json'), 'w') as f:
        json.dump(users, f)

    with open(os.path.join(path, 'posts.json'), 'w') as f:
        json.dump(posts, f)

    with open(os.path.join(path, 'comments.json'), 'w') as f:
        json.dump(comments, f)
    
    with open(os.path.join(path, 'follows.json'), 'w') as f:
        json.dump(follows, f)

if __name__ == "__main__":
    # 生成数据
    users = generate_users(num_users)
    posts = generate_posts(num_posts, [user["id"] for user in users])
    comments = generate_comments(num_comments, [user["id"] for user in users], [post["id"] for post in posts])
    follows = generate_follows(users)
    
    persist_data(users, posts, comments, follows, path=f'datasets/fsnb{scale_fator}')
    
    print("Data generation completed.")
