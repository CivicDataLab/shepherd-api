"""
Django settings for dataplatform project.

Generated by 'django-admin startproject' using Django 3.2.8.

For more information on this file, see
https://docs.djangoproject.com/en/3.2/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/3.2/ref/settings/
"""

from pathlib import Path
import os

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-3+2$eq9d(6o(u^@u9*&5!a_#t6(5h&+jf2w%(e!hw!4_b-qm7v'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

# ALLOWED_HOSTS = ['13.232.239.70', '127.0.0.1', 'pipeline.ndp.civicdatalab.in', 'localhost']
ALLOWED_HOSTS = ['*']

CORS_ALLOW_ALL_ORIGINS = True
CORS_ORIGIN_WHITELIST = (
       'http://localhost:3000',
)

CORS_ALLOW_HEADERS = ['*']

CSRF_TRUSTED_ORIGINS = [
                           'http://13.232.239.70:3000/',
                           'http://localhost:3000/',
                           'http://127.0.0.1:3000/',
                        ]

# Application definition

INSTALLED_APPS = [
    'datatransform.apps.DatatransformConfig',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'background_task',
    'corsheaders',
    'django_extensions',
]

MIDDLEWARE = [
    "dataplatform.middleware.SimpleMiddleware",
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'dataplatform.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'dataplatform.wsgi.application'


CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.db.DatabaseCache',
        'LOCATION': 'my_cache_table',
    }
}


# Database
# https://docs.djangoproject.com/en/3.2/ref/settings/#databases

DATABASES = {
    "default": {
        "ENGINE": os.environ.get('DB_ENGINE', "django.db.backends.sqlite3"),
        "NAME": os.environ.get('DB_NAME', BASE_DIR / "db.sqlite3"),
        "USER": os.environ.get('DB_USER', "DB_USER"),
        "PASSWORD": os.environ.get('DB_PASSWORD', 'DB_PASSWORD'),
        "HOST": os.environ.get('DB_HOST', 'DB_HOST'),
        "PORT":os.environ.get('DB_PORT', 'DB_PORT'), 
        
    }
}


# Password validation
# https://docs.djangoproject.com/en/3.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]



# security headers 
# SECURE_BROWSER_XSS_FILTER = True  
# SECURE_HSTS_SECONDS = 31536000 
# SECURE_HSTS_INCLUDE_SUBDOMAINS = True
# SECURE_HSTS_PRELOAD = True 
# SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https') 
# SESSION_COOKIE_SECURE = True
# CSRF_COOKIE_SECURE = True


MAX_ATTEMPTS = 1
# Internationalization
# https://docs.djangoproject.com/en/3.2/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/3.2/howto/static-files/

STATIC_URL = '/static/'

# Default primary key field type
# https://docs.djangoproject.com/en/3.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
