#*********************************************************************
#   Copyright 2019 Regents of the University of California
#   All rights reserved
#*********************************************************************
# When mvn verify is run, this image will be started and stopped.
#   The integration test plugin will start the image on a high local port and set the ENV var it-server.port.
# To run outside of maven:
#   - This may already be running in the default Merritt docker stack.  
#   - If not...
#     docker build -t it-server-image .
#     docker run --rm -it -p 4567:4567 it-server-image

FROM ruby:2.6.6
RUN apt-get update -qq && apt-get install -y build-essential libpq-dev nodejs

# Adding gems
COPY Gemfile Gemfile

RUN gem install bundler
RUN bundle install --jobs 20 --retry 5
# Adding project files

COPY *.rb .

EXPOSE 4567
CMD ["ruby", "app.rb"]
