var CatClaw = require('./crawler');
const fs = require('fs');

const fd = fs.openSync(`./store${Date.now()}.txt`, 'w+');

if (fd < 0) {
  throw Error('openSync error');
}

const clawer = new CatClaw({
  startUrls: ['http://www.dgtle.com/portal.php?mod=list&catid=3&page=1'],
  callback: function(err, res, utils) {
    // 默认解析文章列表
    if (!err) {
      const $ = res.$;
      // 将文章 url 加入队列
      $('dl').each((idx, ele) => {
        const itemUrl = $(ele).find('dt a').attr('href');
        if (itemUrl) {
          utils.add(itemUrl);
        }
      });
      // 将下一页加入队列
      const nextUrl = $('.nxt').attr('href');
      if (nextUrl) {
        utils.add(nextUrl);
        console.log(`next page add to queue: ${nextUrl}`);
      }
    } else {
      console.log(err);
    }
  },
  maxConcurrency: 5,
  interval: 4000,
  maxRetries: 3,
  timeout: 4000,
  drain: function() {
    console.log('drain...');
  },
  hostsRestricted: ['www.dgtle.com'],
  encoding: 'utf8',
  seenEnable: true,
});

// 解析文章
clawer.use('http://www.dgtle.com/article-(.*)', function (err, res, helper) {
  if (err) {
    console.error('an error occurs in crawling article content');
    console.log(err.message);
    return;
  }
  console.log('解析文章');
  const $ = res.$;
  const type = $('.cr_catdates a').text();
  const title = $('.cr_h1title a').text();
  const content = $('#view_content').text();
  const pageUrl = res.request.href;

  if (!type || !title || !content || !pageUrl) {
    return;
  }

  fs.write(fd, `${title.replace(/[\f\n\r\t]/g, '')}\t${type.replace(/[\f\n\r\t]/g, '')}\t${content.replace(/[\f\n\r\t]/g, '')}\t${pageUrl}\n`, (err) => {
    if (err) {
      console.log('[ERROR] fs write error');
    }
  });
});

clawer.start();
