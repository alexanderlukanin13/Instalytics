Instalytics
===========

Data Science for Instagram


Local Setup
-----------

Store AWS credentials in `~/.aws/credentials`, like this:

    [default]
    aws_access_key_id = <key_id>
    aws_secret_access_key = <secret>
    region = <region>


Testing (manual)
----------------

### Step 1

Get location: Zürich, Switzerland:

    python run.py --no-proxy get location 214122044

It should print something like this:

    For 214122044 the following 24 pictures were added: ['Bsp7s09gvvM', 'Bsp7qfyAtwV', ...

### Step 2

Get picture (use ID from previous step):

    python run.py --no-proxy get picture Bsp7s09gvvM

It should store a picture in `downloads/pictures`.

### Step 3

Get user:

    python run.py --no-proxy get user atelierpeternitz

It should print something like this:

    Local file storage: ./downloads/json/user/atelierpeternitz has been created
