import crypto from 'crypto';

export function generateSecret(): Promise<string> {
    return new Promise((resolve, reject) => {
        crypto.randomBytes(48, (err, b) => {
            if (err) {
                return reject(err);
            }
            resolve(b.toString("base64"));
        });
    })
}

export function ankiMustache(s: string, d: Record<string, any> = {}, front: string = ""): string {
  s = s.replace(/{{FrontSide}}/g, front.replace(/@html\n/g, ""))

  for (const [k, v] of Object.entries(d)) {
      if (typeof v === "string") {
          s = s.replace(
              new RegExp(`{{(\\S+:)?${escapeRegExp(k)}}}`, "g"),
              v.replace(/^@[^\n]+\n/gs, "")
          );
      }
  }

  s = s.replace(/{{#(\S+)}}([^]*){{\1}}/gs, (m, p1, p2) => {
      return Object.keys(d).includes(p1) ? p2 : "";
  });

  s = s.replace(/{{[^}]+}}/g, "");

  return s;
}

export function shuffle(a: any[]) {
  for (let i = a.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

export function escapeRegExp(s: string) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');  // $& means the whole matched string
}

export function chunk<T>(array: T[], size: number): T[][] {
  var results: T[][] = [],
    length = Math.ceil(array.length / size);

  for (var i = 0; i < length; i++) {
    results.push(array.slice(i * size, (i + 1) * size));
  }
  return results;
}

declare global {
  interface Array<T> {
    distinctBy<U extends string | number>(mapFn: (el: T) => U): T[];
    mapAsync<U>(mapFn: (value: T, index: number, array: T[]) => Promise<U>): Promise<U[]>;
  }
}

Array.prototype.distinctBy = function(mapFn) {
  const uniqueKeys = new Set(this.map(mapFn));
  return this.filter((el) => uniqueKeys.has(mapFn(el)));
}

Array.prototype.mapAsync = async function(mapFn) {
  return await Promise.all(this.map(async (value, index, array) => {
    try {
      return await mapFn(value, index, array);
    } catch(e) {
      throw e;
    }
  }));
}