import http from 'k6/http';
import { check, fail, group, sleep } from 'k6';

export const options = {
  scenarios: {
    journey: {
      executor: 'constant-vus',
      vus: 1,
      duration: '30s',
    },
  },

  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1000'],
    checks: ['rate>0.99'],
  },

  tags: {
    test_type: 'journey',
    service: 'ngradar_website',
  },
};

const baseUrl = (
  __ENV.K6_BASE_URL || 'http://ngradar-website:8000'
).replace(/\/$/, '');

const username = __ENV.K6_USERNAME;
const password = __ENV.K6_PASSWORD;

// Required image UUID supplied by the test runner. In docker compose environment
// Make sure you pick an image ID from your DB and add it to your .env
const imageId = __ENV.K6_IMAGE_ID;

const waveformField = __ENV.K6_WAVEFORM_FIELD || 'waveform';
// const waveformValue = __ENV.K6_WAVEFORM_VALUE || '48';
const waveformValue = __ENV.K6_WAVEFORM_VALUE || 'W48';


function extractCsrfToken(body) {
  const patterns = [
    /name=["']csrfmiddlewaretoken["'][^>]*value=["']([^"']+)["']/i,
    /value=["']([^"']+)["'][^>]*name=["']csrfmiddlewaretoken["']/i,
  ];

  for (const pattern of patterns) {
    const match = body.match(pattern);

    if (match) {
      return match[1];
    }
  }

  return null;
}

function checkAuthenticatedPage(response, name) {
  check(response, {
    [`${name} returns 200`]: (r) => r.status === 200,

    [`${name} is not login page`]: (r) =>
      !r.url.includes('/login/'),
  });
}

export default function () {
  if (!username || !password) {
    fail(
      'K6_USERNAME and K6_PASSWORD must be provided'
    );
  }

  if (!imageId) {
    fail(
      'K6_IMAGE_ID must be provided, for example: ' +
      'K6_IMAGE_ID=3f3d44e5-553d-452f-a938-26b0b3651ccb'
    );
  }

  // Basic validation to catch accidental full URLs or paths.
  const validUuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

  if (!validUuid.test(imageId)) {
    fail(
      `K6_IMAGE_ID is not a valid UUID: ${imageId}`
    );
  }

  const imagePath = `/home/image/${imageId}/`;

  let homeResponse;
  let dashboardResponse;

// ------------------------------------------------------------
  group('login', () => {
    const loginPage = http.get(`${baseUrl}/login/`, {
      tags: {
        endpoint: 'login_page',
      },
    });

    check(loginPage, {
      'login page returns 200': (r) => r.status === 200,
    });

    const loginCsrfToken = extractCsrfToken(loginPage.body);

    if (!loginCsrfToken) {
      fail(
        'Could not find Django CSRF token on /login/'
      );
    }

    const loginResponse = http.post(
      `${baseUrl}/login/`,
      {
        username,
        password,
        csrfmiddlewaretoken: loginCsrfToken,
        next: '/home/',
      },
      {
        headers: {
          Referer: `${baseUrl}/login/`,
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        redirects: 0,
        tags: {
          endpoint: 'login_submit',
        },
      }
    );

    check(loginResponse, {
      'login returns redirect': (r) =>
        r.status === 302 || r.status === 303,

      'login sets Django session cookie': (r) =>
        Boolean(
          r.cookies.sessionid &&
          r.cookies.sessionid.length > 0
        ),

      'login redirects to home': (r) => {
        const location = r.headers.Location || '';

        return (
          location === '/home/' ||
          location.endsWith('/home/')
        );
      },
    });

    if (
      loginResponse.status !== 302 &&
      loginResponse.status !== 303
    ) {
      fail(
        `Login failed with HTTP ${loginResponse.status}`
      );
    }
  });

// ------------------------------------------------------------
  group('home-before-submit', () => {
    homeResponse = http.get(`${baseUrl}/home/`, {
      tags: {
        endpoint: 'home_before_submit',
      },
    });

    checkAuthenticatedPage(
      homeResponse,
      'home page'
    );
  });

  const csrfToken = extractCsrfToken(homeResponse.body);

  if (!csrfToken) {
    fail(
      'Could not find Django CSRF token on /home/'
    );
  }

// ------------------------------------------------------------
//   group('submit-waveform', () => {
//     const waveformResponse = http.post(
//       `${baseUrl}/home/submit-waveform/`,
//       {
//         [waveformField]: waveformValue,
//         csrfmiddlewaretoken: csrfToken,
//       },
//       {
//         headers: {
//           Referer: `${baseUrl}/home/`,
//           'Content-Type': 'application/x-www-form-urlencoded',
//         },
//         redirects: 0,
//         tags: {
//           endpoint: 'submit_waveform',
//         },
//       }
//     );
//
//     check(waveformResponse, {
//       'waveform submission redirects': (r) =>
//         r.status === 302 || r.status === 303,
//
//       'waveform redirects to home': (r) => {
//         const location = r.headers.Location || '';
//
//         return (
//           location === '/home/' ||
//           location.endsWith('/home/')
//         );
//       },
//     });
//
//     if (
//       waveformResponse.status !== 302 &&
//       waveformResponse.status !== 303
//     ) {
//       fail(
//         `Waveform submission failed with HTTP ${waveformResponse.status}`
//       );
//     }
//   });

// ------------------------------------------------------------
  group('submit-waveform', () => {
    const waveformResponse = http.post(
      `${baseUrl}/home/submit-waveform/`,
      {
        [waveformField]: waveformValue,
        csrfmiddlewaretoken: csrfToken,
      },
      {
        headers: {
          Referer: `${baseUrl}/home/`,
          'Content-Type':
            'application/x-www-form-urlencoded',
        },
        redirects: 0,
        tags: {
          endpoint: 'submit_waveform',
        },
        responseCallback: http.expectedStatuses(
          202,
          409
        ),
      }
    );

    const isAccepted =
      waveformResponse.status === 202;

    const isLocked =
      waveformResponse.status === 409;

    check(waveformResponse, {
      'waveform submission is accepted or locked':
        () => isAccepted || isLocked,
    });

    let responseBody;

    try {
      responseBody =
        waveformResponse.json();
    } catch (error) {
      fail(
        `Waveform response was not valid JSON: ` +
        `status=${waveformResponse.status} ` +
        `body=${waveformResponse.body}`
      );
    }

    if (isAccepted) {
      check(responseBody, {
        'waveform response reports success':
          (data) =>
            data.error === false,

        'waveform response contains event UUID':
          (data) =>
            Boolean(data.event_uuid),

        'waveform response contains message':
          (data) =>
            Boolean(data.message),
      });

      console.log(
        `Waveform ${waveformValue} accepted: ` +
        `${responseBody.event_uuid}`
      );

    } else if (isLocked) {
      check(responseBody, {
        'locked response reports error':
          (data) =>
            data.error === true,

        'locked response contains message':
          (data) =>
            Boolean(data.message),
      });

      console.warn(
        `Waveform submission locked: ` +
        `${responseBody.message}`
      );

    } else {
      fail(
        `Waveform submission failed: ` +
        `status=${waveformResponse.status} ` +
        `body=${waveformResponse.body}`
      );
    }
  });

// ------------------------------------------------------------
  group('home-after-submit', () => {
    const response = http.get(`${baseUrl}/home/`, {
      tags: {
        endpoint: 'home_after_submit',
      },
    });

    checkAuthenticatedPage(
      response,
      'home after waveform submission'
    );

    /*
     * The browser opens /events/stream/ through JavaScript.
     * This journey intentionally does not open the long-lived
     * SSE connection.
     */
  });

// ------------------------------------------------------------
  group('dashboard', () => {
    dashboardResponse = http.get(`${baseUrl}/dashboard/`, {
      tags: {
        endpoint: 'dashboard',
      },
    });

    checkAuthenticatedPage(
      dashboardResponse,
      'dashboard page'
    );
  });

// ------------------------------------------------------------
  group('open-dashboard-image', () => {
    console.log(`Opening image path: ${imagePath}`);

    /*
     * The endpoint is expected to redirect to a signed URL such as:

       http://images.localhost/ddm-images/...png?X-Amz-...

     * redirects: 0 prevents k6 from trying to connect to
     * images.localhost.
     */
    const imagePageResponse = http.get(
      `${baseUrl}${imagePath}`,
      {
        redirects: 0,
        tags: {
          endpoint: 'home_image_redirect',
        },
      }
    );

    const location =
      imagePageResponse.headers.Location || '';

    check(imagePageResponse, {
      'image endpoint returns redirect': (r) =>
        r.status === 301 ||
        r.status === 302 ||
        r.status === 303 ||
        r.status === 307 ||
        r.status === 308,

      'image redirect points to images.localhost': () =>
        location.startsWith(
          'http://images.localhost/'
        ) ||
        location.startsWith(
          'https://images.localhost/'
        ),

      'image redirect contains signed URL': () =>
        location.includes('X-Amz-Signature='),

      'image redirect contains image path': () =>
        location.includes('/ddm-images/'),
    });

    if (
      imagePageResponse.status < 300 ||
      imagePageResponse.status >= 400
    ) {
      fail(
        `Expected ${imagePath} to redirect, but received HTTP ${imagePageResponse.status}`
      );
    }
  });

  let finalHomeResponse;

// ------------------------------------------------------------
  group('return-home', () => {
    finalHomeResponse = http.get(`${baseUrl}/home/`, {
      tags: {
        endpoint: 'home_final',
      },
    });

    check(finalHomeResponse, {
      'final home page returns 200': (r) =>
        r.status === 200,

      'final home page is authenticated': (r) =>
        !r.url.includes('/login/'),
    });
  });

 // ------------------------------------------------------------ 
  group('logout', () => {
    const logoutCsrfToken = extractCsrfToken(
      finalHomeResponse.body
    );

    if (!logoutCsrfToken) {
      fail(
        'Could not find Django CSRF token on final /home/'
      );
    }

    const logoutResponse = http.post(
      `${baseUrl}/logout/`,
      {
        csrfmiddlewaretoken: logoutCsrfToken,
      },
      {
        headers: {
          Referer: `${baseUrl}/home/`,
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        redirects: 0,
        tags: {
          endpoint: 'logout',
        },
      }
    );

    const location = logoutResponse.headers.Location || '';

    check(logoutResponse, {
      'logout returns redirect': (r) =>
        r.status === 302 || r.status === 303,

      'logout redirects to /login/': () =>
        location === '/login/' ||
        location.endsWith('/login/'),
    });

    if (
      logoutResponse.status !== 302 &&
      logoutResponse.status !== 303
    ) {
      fail(
        `Logout failed with HTTP ${logoutResponse.status}`
      );
    }

    if (
      location !== '/login/' &&
      !location.endsWith('/login/')
    ) {
      fail(
        `Logout redirected to an unexpected location: ${location}`
      );
    }

    const postLogoutHomeResponse = http.get(
    `${baseUrl}/home/`,
    {
        redirects: 0,
        tags: {
        endpoint: 'home_after_logout',
        },
    }
    );

    const postLogoutLocation_home =
    postLogoutHomeResponse.headers.Location || '';

    check(postLogoutHomeResponse, {
    'home is not accessible after logout': (r) =>
        r.status === 302 || r.status === 303,

    'home redirects to login after logout': () =>
        postLogoutLocation_home === '/login/?next=/home/' ||
        postLogoutLocation_home.endsWith('/login/?next=/home/'),
    });

    const postLogoutDashboardResponse = http.get(
    `${baseUrl}/dashboard/`,
    {
        redirects: 0,
        tags: {
        endpoint: 'dashboard_after_logout',
        },
    }
    );

    const postLogoutLocation_dashboard =
    postLogoutDashboardResponse.headers.Location || '';

    check(postLogoutDashboardResponse, {
    'dashboard is not accessible after logout': (r) =>
        r.status === 302 || r.status === 303,

    'dashboard redirects to login after logout': () =>
        postLogoutLocation_dashboard === '/login/?next=/dashboard/' ||
        postLogoutLocation_dashboard.endsWith('/login/?next=/dashboard/'),
    });

  });


  sleep(1);
}