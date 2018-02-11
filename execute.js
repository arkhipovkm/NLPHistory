
var group = args.group;
//var group = -15548215;

var posts = API.wall.get({
    "access_token": "6faaf0f33949745684fdf0a59dfac78bd41562a3da95c009c0fb155458fa55b0d0199701f3309b9878d19",
    "owner_id": group,
    "count": 10,
    "v": 5.71,
    "offset": 0
});
var i = 0;
var response = {};
response.items = [];

while (posts.items[i]) {
    var c = API.wall.getComments({
        "access_token": "6faaf0f33949745684fdf0a59dfac78bd41562a3da95c009c0fb155458fa55b0d0199701f3309b9878d19",
        "owner_id": group,
        "post_id": posts.items[i].id,
        "preview_length": 0,
        "need_likes": 1,
        "extended": 1,
        "fields": "bdate,sex,city,country",
        "count": 10,
        "v": 5.71,
        "offset": 0
    });

    response.items.push({
        "group": group,
        "post": posts.items[i].id,
        "comments": c.items,
        "profiles": c.profiles
    });

    i = i + 1;
}

response.count = response.items.length;

return response;